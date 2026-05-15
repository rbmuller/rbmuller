// Generates etl-diagram.svg with Excalidraw-style sketchy strokes via rough.js.
// Run with: node build-diagram.mjs
import rough from "roughjs/bundled/rough.esm.js";
import { JSDOM } from "jsdom";
import fs from "node:fs";

const W = 1200, H = 560;
const dom = new JSDOM(
  `<!DOCTYPE html><body><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${W} ${H}" width="100%"></svg></body>`,
  { contentType: "text/html" }
);
const doc = dom.window.document;
const svg = doc.querySelector("svg");
svg.setAttribute("font-family", "ui-sans-serif, -apple-system, BlinkMacSystemFont, 'Segoe UI', Inter, sans-serif");
svg.setAttribute("role", "img");
svg.setAttribute("aria-label", "ETL pipeline: Sources to Ingest to Raw to Transform to Warehouse, branching into Analytics and ML, orchestrated by Airflow / Cloud Composer");

const rc = rough.svg(svg);

const STROKE = "#1e1e1e";
const SUB = "#6b6b6b";

// Excalidraw-ish defaults: hand-drawn look comes from roughness + bowing
const roughBase = {
  stroke: STROKE,
  strokeWidth: 1.8,
  roughness: 1.6,
  bowing: 1.4,
  fillStyle: "solid",
  seed: 0, // overridden per-shape so each box has its own consistent wobble
};

function rect(x, y, w, h, fill, seed) {
  const node = rc.rectangle(x, y, w, h, { ...roughBase, fill, seed });
  svg.appendChild(node);
}

// Cylinder = top ellipse + body rect + bottom curve. We emulate with one rect + an arc on top.
function cylinder(cx, cy, w, h, fill, seed) {
  const x = cx - w / 2;
  const y = cy - h / 2;
  const ellipseH = 28;
  // body (solid fill)
  const body = rc.rectangle(x, y + ellipseH / 2, w, h - ellipseH / 2, { ...roughBase, fill, seed });
  svg.appendChild(body);
  // top ellipse (sits on top, filled to match)
  const topFill = rc.ellipse(cx, y + ellipseH / 2, w, ellipseH, { ...roughBase, fill, seed: seed + 1 });
  svg.appendChild(topFill);
  // bottom curve (just the lower half of an ellipse to suggest 3D)
  const bottomArc = rc.path(
    `M ${x} ${y + h - ellipseH / 2} A ${w / 2} ${ellipseH / 2} 0 0 0 ${x + w} ${y + h - ellipseH / 2}`,
    { ...roughBase, fill: "none", seed: seed + 2 }
  );
  svg.appendChild(bottomArc);
}

function arrow(x1, y1, x2, y2, opts = {}) {
  const line = rc.line(x1, y1, x2, y2, { ...roughBase, ...opts });
  svg.appendChild(line);
  // arrowhead — small filled triangle pointing along the vector
  const angle = Math.atan2(y2 - y1, x2 - x1);
  const len = 14;
  const hx1 = x2 - len * Math.cos(angle - Math.PI / 9);
  const hy1 = y2 - len * Math.sin(angle - Math.PI / 9);
  const hx2 = x2 - len * Math.cos(angle + Math.PI / 9);
  const hy2 = y2 - len * Math.sin(angle + Math.PI / 9);
  const head = rc.polygon(
    [[x2, y2], [hx1, hy1], [hx2, hy2]],
    { ...roughBase, fill: opts.stroke ?? STROKE, fillStyle: "solid", strokeWidth: 1.2, seed: (opts.seed ?? 0) + 7 }
  );
  svg.appendChild(head);
}

function dashedArrow(path, opts = {}) {
  // For curved dashed arrows, we draw the path with rough + strokeLineDash
  const p = rc.path(path, { ...roughBase, strokeLineDash: [8, 5], strokeWidth: 1.6, ...opts });
  svg.appendChild(p);
}

function label(x, y, text, { size = 22, weight = 700, fill = STROKE, italic = false } = {}) {
  const t = doc.createElementNS("http://www.w3.org/2000/svg", "text");
  t.setAttribute("x", x);
  t.setAttribute("y", y);
  t.setAttribute("text-anchor", "middle");
  t.setAttribute("font-size", size);
  t.setAttribute("font-weight", weight);
  t.setAttribute("fill", fill);
  if (italic) t.setAttribute("font-style", "italic");
  t.textContent = text;
  svg.appendChild(t);
}

// =========================================================================
// LAYOUT (centers)
// =========================================================================
const ROW1_Y = 185;     // main pipeline
const ROW2_Y = 445;     // analytics + ML branches
const ORCH_Y = 465;     // airflow box
const NODE_W = 160;
const NODE_H = 130;

// 5-stage pipeline. Compute x centers spaced across width with margin.
const xSources   = 135;
const xIngest    = 345;
const xRaw       = 555;
const xTransform = 765;
const xWarehouse = 1010;

// Excalidraw color palette
const C_YELLOW = "#ffec99";
const C_GREEN  = "#b2f2bb";
const C_BLUE   = "#a5d8ff";
const C_ORANGE = "#ffd8a8";
const C_PINK   = "#fcc2d7";
const C_PURPLE = "#d0bfff";
const C_ORCH   = "#fff3bf";

// =========================================================================
// MAIN PIPELINE
// =========================================================================
cylinder(xSources, ROW1_Y, 150, NODE_H, C_YELLOW, 11);
label(xSources, ROW1_Y - 8, "Sources", { size: 24 });
label(xSources, ROW1_Y + 22, "APIs · DBs · Events", { size: 15, weight: 500, fill: SUB });

arrow(xSources + 80, ROW1_Y, xIngest - NODE_W / 2 - 5, ROW1_Y, { seed: 20 });

rect(xIngest - NODE_W / 2, ROW1_Y - NODE_H / 2, NODE_W, NODE_H, C_GREEN, 30);
label(xIngest, ROW1_Y - 28, "Ingest", { size: 24 });
label(xIngest, ROW1_Y + 4, "Go · Python", { size: 15, weight: 500, fill: SUB });
label(xIngest, ROW1_Y + 28, "500k+ records / day", { size: 15, weight: 500, fill: SUB });

arrow(xIngest + NODE_W / 2 + 5, ROW1_Y, xRaw - 80, ROW1_Y, { seed: 40 });

cylinder(xRaw, ROW1_Y, 160, NODE_H, C_BLUE, 50);
label(xRaw, ROW1_Y - 8, "Raw", { size: 24 });
label(xRaw, ROW1_Y + 22, "Databricks · BigQuery", { size: 15, weight: 500, fill: SUB });

arrow(xRaw + 80, ROW1_Y, xTransform - NODE_W / 2 - 5, ROW1_Y, { seed: 60 });

rect(xTransform - NODE_W / 2, ROW1_Y - NODE_H / 2, NODE_W, NODE_H, C_GREEN, 70);
label(xTransform, ROW1_Y - 18, "Transform", { size: 24 });
label(xTransform, ROW1_Y + 14, "dbt", { size: 15, weight: 500, fill: SUB });
label(xTransform, ROW1_Y + 36, "PySpark", { size: 15, weight: 500, fill: SUB });

arrow(xTransform + NODE_W / 2 + 5, ROW1_Y, xWarehouse - 90, ROW1_Y, { seed: 80 });

cylinder(xWarehouse, ROW1_Y, 180, NODE_H, C_ORANGE, 90);
label(xWarehouse, ROW1_Y + 0, "Warehouse", { size: 24 });
label(xWarehouse, ROW1_Y + 30, "BigQuery · Databricks", { size: 15, weight: 500, fill: SUB });

// =========================================================================
// BRANCHES (Warehouse → Analytics + ML)
// =========================================================================
const xAnalytics = 770;
const xML = 1100;

// Vertical down from warehouse, then split
arrow(xWarehouse, ROW1_Y + NODE_H / 2 + 5, xWarehouse, 310, { seed: 100, strokeWidth: 1.6 });
// Horizontal split bar (no arrow heads, just a connector)
const splitBar = rc.line(xAnalytics, 310, xML, 310, { ...roughBase, seed: 105 });
svg.appendChild(splitBar);
// Down arrows to each branch
arrow(xAnalytics, 312, xAnalytics, ROW2_Y - NODE_H / 2 - 5, { seed: 110 });
arrow(xML, 312, xML, ROW2_Y - NODE_H / 2 - 5, { seed: 115 });

rect(xAnalytics - NODE_W / 2, ROW2_Y - NODE_H / 2, NODE_W, NODE_H, C_PINK, 120);
label(xAnalytics, ROW2_Y - 18, "Analytics", { size: 24 });
label(xAnalytics, ROW2_Y + 14, "Superset", { size: 15, weight: 500, fill: SUB });
label(xAnalytics, ROW2_Y + 36, "dashboards", { size: 15, weight: 500, fill: SUB });

rect(xML - NODE_W / 2, ROW2_Y - NODE_H / 2, NODE_W, NODE_H, C_PURPLE, 130);
label(xML, ROW2_Y - 18, "ML", { size: 24 });
label(xML, ROW2_Y + 14, "datatrax (Go)", { size: 15, weight: 500, fill: SUB });
label(xML, ROW2_Y + 36, "7 algorithms", { size: 15, weight: 500, fill: SUB });

// =========================================================================
// ORCHESTRATOR (Airflow) — dashed border
// =========================================================================
const orchX = 200;
const orchY = ORCH_Y - 45;
const orchW = 320;
const orchH = 90;
const orchBox = rc.rectangle(orchX, orchY, orchW, orchH, {
  ...roughBase,
  fill: C_ORCH,
  stroke: "#9a7d0a",
  strokeWidth: 2,
  strokeLineDash: [8, 5],
  seed: 200,
});
svg.appendChild(orchBox);
label(orchX + orchW / 2, ORCH_Y - 10, "Airflow · Cloud Composer", { size: 19, weight: 700, fill: "#3d3a1f" });
label(orchX + orchW / 2, ORCH_Y + 16, "orchestrates", { size: 14, weight: 500, fill: SUB, italic: true });

// Dashed arrows from Airflow up to Ingest and Transform
dashedArrow(`M ${orchX + 90} ${orchY} Q ${orchX + 90} ${340} ${xIngest} ${ROW1_Y + NODE_H / 2 + 8}`, {
  stroke: "#9a7d0a",
  seed: 210,
});
dashedArrow(`M ${orchX + orchW - 90} ${orchY} Q ${600} ${340} ${xTransform} ${ROW1_Y + NODE_H / 2 + 8}`, {
  stroke: "#9a7d0a",
  seed: 220,
});

// =========================================================================
// SERIALIZE
// =========================================================================
const xml = `<?xml version="1.0" encoding="UTF-8"?>\n${svg.outerHTML}`;
fs.writeFileSync("etl-diagram.svg", xml);
console.log(`Wrote etl-diagram.svg (${xml.length} bytes)`);
