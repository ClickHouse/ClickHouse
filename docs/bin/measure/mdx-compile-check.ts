// Compiles every MDX file under the given directories with Sätteri exactly as
// the site does (same features, same normalisation) and lists the failures
// with file names. Usage: node bin/measure/mdx-compile-check.ts reference guides ...
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { mdxToJs } from "satteri";
import { normalizeMdx } from "../../src/plugins/vite-mintlify-snippets.ts";
import { SATTERI_FEATURES } from "../../src/plugins/satteri-features.ts";

const root = process.cwd();
const dirs = process.argv.slice(2).filter((a) => !a.startsWith("--")).length ? process.argv.slice(2).filter((a) => !a.startsWith("--")) : ["."];
const indexFile = path.join(root, "src/generated/import-index.json");
const importIndex = fs.existsSync(indexFile) ? (JSON.parse(fs.readFileSync(indexFile, "utf8")) as Record<string, string>) : {};

const files: string[] = [];
(function walk(dir: string) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    if (e.name === "node_modules" || e.name === "dist" || e.name === "tmp" || e.name === ".astro") continue;
    const p = path.join(dir, e.name);
    if (e.isDirectory()) walk(p);
    else if (e.name.endsWith(".mdx")) files.push(p);
  }
})(path.resolve(root, dirs[0]));
for (const d of dirs.slice(1)) (function walk(dir: string) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) walk(p);
    else if (e.name.endsWith(".mdx")) files.push(p);
  }
})(path.resolve(root, d));

// --refs: also report PascalCase tags that are neither imported/defined in the
// file nor provided as MDX globals (they fail at render time, not parse time).
const checkRefs = process.argv.includes("--refs");
const GLOBALS = new Set([
  "Aside", "CardGrid", "PackageManagers", "Render", "TabItem",
  "Note", "Info", "Tip", "Warning", "Danger", "Check", "Tabs", "Tab", "Steps", "Step",
  "Card", "CardGroup", "Accordion", "AccordionGroup", "Expandable", "Frame", "Badge", "Tooltip", "Columns", "Update", "Icon", "View", "Visibility", "CodeBlock",
  "Fragment",
]);
const undefinedRefs = new Map<string, string[]>();
function stripCode(src: string): string {
  return src.replace(/```[\s\S]*?```/g, "").replace(/`[^`\n]*`/g, "");
}
import { declared as declaredIn } from "../../src/plugins/vite-mintlify-snippets.ts";

let failed = 0;
const byMessage = new Map<string, number>();
const start = Date.now();
for (const file of files) {
  const src = fs.readFileSync(file, "utf8");
  try {
    const code = normalizeMdx(src, importIndex);
    await mdxToJs(code, { features: SATTERI_FEATURES, fileURL: pathToFileURL(file) });
    if (checkRefs) {
      const body = stripCode(code);
      const tags = new Set([...body.matchAll(/<([A-Z][A-Za-z0-9_]*)[\s/>]/g)].map((m) => m[1]));
      for (const t of tags) {
        if (GLOBALS.has(t) || declaredIn(code, t)) continue;
        undefinedRefs.set(t, [...(undefinedRefs.get(t) ?? []), path.relative(root, file)]);
      }
    }
  } catch (err) {
    failed++;
    const msg = String((err as Error).message ?? err).split("\n")[0];
    const key = msg.replace(/^\d+:\d+:\s*/, "").slice(0, 90);
    byMessage.set(key, (byMessage.get(key) ?? 0) + 1);
    console.log(`${path.relative(root, file)}: ${msg.slice(0, 160)}`);
  }
}
console.log(`\nmdx-compile-check: ${files.length - failed}/${files.length} files compile (${((Date.now() - start) / 1000).toFixed(1)}s)`);
for (const [k, n] of [...byMessage.entries()].sort((a, b) => b[1] - a[1])) console.log(`  ${n}x ${k}`);
if (checkRefs) {
  const total = [...undefinedRefs.values()].reduce((n, f) => n + f.length, 0);
  console.log(`\nundefined component references: ${undefinedRefs.size} names in ${total} files`);
  for (const [name, files] of [...undefinedRefs.entries()].sort((a, b) => b[1].length - a[1].length)) {
    console.log(`  ${name} (${files.length}): ${files.slice(0, 3).join(", ")}${files.length > 3 ? ", ..." : ""}`);
  }
  if (undefinedRefs.size) process.exitCode = 1;
}
process.exitCode = failed ? 1 : process.exitCode ?? 0;
