// Every explicit `{#id}` heading anchor in the source MDX must exist as an
// element id in the built HTML. Usage: node bin/measure/anchor-parity.ts [dist]
import fs from "node:fs";
import path from "node:path";

const root = process.cwd();
const dist = path.resolve(root, process.argv[2] ?? "dist");
// Astro writes the site at dist/ root; `base` is only a serving prefix.

function* walk(dir: string): Generator<string> {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) yield* walk(p);
    else yield p;
  }
}

// dist/<base>/<id>/index.html  ->  source <id>.mdx
const pages = [...walk(dist)].filter((f) => f.endsWith("index.html"));
let checked = 0, missingTotal = 0;
const report: string[] = [];
for (const html of pages) {
  const rel = path.relative(dist, path.dirname(html)).replaceAll(path.sep, "/");
  const id = rel === "" ? "index" : rel;
  const src = path.join(root, `${id}.mdx`);
  if (!fs.existsSync(src)) continue;
  // Headings inside fenced code or MDX comments are not rendered; skip them.
  const source = fs
    .readFileSync(src, "utf8")
    .replace(/```[\s\S]*?```/g, "")
    .replace(/\{\/\*[\s\S]*?\*\/\}/g, "");
  const anchors = [...source.matchAll(/^#{1,6}\s.*\{#([^}\s]+)\}\s*$/gm)].map((m) => m[1]);
  if (!anchors.length) continue;
  const out = fs.readFileSync(html, "utf8");
  const ids = new Set([...out.matchAll(/\sid="([^"]+)"/g)].map((m) => m[1]));
  const missing = anchors.filter((a) => !ids.has(a));
  checked += anchors.length;
  missingTotal += missing.length;
  if (missing.length) report.push(`${id}: ${missing.length}/${anchors.length} missing, e.g. ${missing.slice(0, 5).join(", ")}`);
}
console.log(`anchor-parity: ${checked - missingTotal}/${checked} anchors present across ${pages.length} pages`);
for (const line of report) console.log("  " + line);
process.exitCode = missingTotal ? 1 : 0;
