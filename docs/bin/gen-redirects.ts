// Turns docs/_site/redirects.json (Mintlify shape) into a Cloudflare-style
// `__redirects` file for the docs Worker (redirects-in-workers), prefixing the
// /docs base on both sides. Fragment sources cannot be matched server-side and
// are written to src/generated/anchor-redirects.json for the client script.
// Usage: node bin/gen-redirects.ts [outDir]
import fs from "node:fs";
import path from "node:path";

const root = process.cwd();
const outDir = path.resolve(root, process.argv[2] ?? "dist");
const base = "/docs";
type R = { source: string; destination: string };
const input = JSON.parse(fs.readFileSync(path.join(root, "_site/redirects.json"), "utf8")) as R[];

const withBase = (p: string) => (/^https?:\/\//.test(p) ? p : p === base || p.startsWith(base + "/") ? p : base + p);
const lines: string[] = [];
const anchors: Array<{ source: string; destination: string }> = [];
const anomalies: string[] = [];
const seen = new Map<string, string>();
let splats = 0;

for (const r of input) {
  let src = r.source.trim();
  let dst = r.destination.trim();
  if (!src.startsWith("/")) { anomalies.push(`non-absolute source: ${src}`); continue; }
  if (src.startsWith(base + "/")) anomalies.push(`source already prefixed: ${src}`);
  if (src.includes("#")) { anchors.push({ source: withBase(src), destination: withBase(dst) }); continue; }
  // Mintlify `:path*` splats -> Cloudflare `*` / `:splat`.
  if (/:[A-Za-z]+\*/.test(src)) {
    src = src.replace(/\/:[A-Za-z]+\*$/, "/*");
    dst = dst.replace(/\/:[A-Za-z]+\*$/, "/:splat");
    splats++;
  }
  const from = withBase(src);
  const to = withBase(dst);
  if (from === to) { anomalies.push(`self redirect: ${from}`); continue; }
  const prev = seen.get(from);
  if (prev && prev !== to) { anomalies.push(`duplicate source ${from}: ${prev} vs ${to}`); continue; }
  if (prev) continue;
  seen.set(from, to);
  lines.push(`${from} ${to} 301`);
}

// Mintlify served `folder/index.mdx` at `/folder/index` too; the canonical is `/folder`.
const LOCALES = ["ar", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh"];
const SECTIONS = ["get-started", "concepts", "guides", "reference", "products", "clickstack", "integrations", "resources", "chdb"];
let indexRules = 0;
for (const tree of ["", ...LOCALES]) {
  for (const section of SECTIONS) {
    const dir = path.join(root, tree, section);
    if (!fs.existsSync(dir)) continue;
    (function walk(d: string) {
      for (const e of fs.readdirSync(d, { withFileTypes: true })) {
        if (e.name.startsWith("_")) continue;
        const p = path.join(d, e.name);
        if (e.isDirectory()) walk(p);
        else if (e.name === "index.mdx") {
          const rel = path.relative(path.join(root, tree), path.dirname(p)).replaceAll(path.sep, "/");
          const prefix = tree ? `${base}/${tree.toLowerCase()}` : base;
          const from = `${prefix}/${rel}/index`;
          if (!seen.has(from)) { seen.set(from, `${prefix}/${rel}`); lines.push(`${from} ${prefix}/${rel} 301`); indexRules++; }
        }
      }
    })(dir);
  }
}

fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(path.join(outDir, "__redirects"), lines.join("\n") + "\n");
fs.mkdirSync(path.join(root, "src/generated"), { recursive: true });
fs.writeFileSync(path.join(root, "src/generated/anchor-redirects.json"), JSON.stringify(anchors, null, 2) + "\n");
fs.mkdirSync(path.join(root, "reports"), { recursive: true });
fs.writeFileSync(path.join(root, "reports/redirects-anomalies.txt"), anomalies.join("\n") + "\n");
console.log(`gen-redirects: ${lines.length} rules (${splats} splats, ${indexRules} index-page rules) -> ${path.relative(root, path.join(outDir, "__redirects"))}; ${anchors.length} fragment sources -> client; ${anomalies.length} anomalies -> reports/redirects-anomalies.txt`);
