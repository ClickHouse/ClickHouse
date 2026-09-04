// Post-build pipeline (runs as `pnpm postbuild`):
//  1. prune `.mdx` source twins (the Worker serves the `.md` twin for both),
//  2. rewrite any stray site-root URLs under /docs,
//  3. generate `__redirects` next to the site,
//  4. nest the site under <outDir>/docs so asset paths equal request paths.
// Honours DOCS_OUT_DIR like astro.config.ts.
import fs from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";

const root = process.cwd();
const outDir = path.resolve(root, process.env.DOCS_OUT_DIR ?? "dist");
const nested = path.join(outDir, "docs");
if (fs.existsSync(nested) && fs.existsSync(path.join(nested, "index.html")) && !fs.existsSync(path.join(outDir, "index.html"))) {
  console.log("postbuild: already nested, nothing to do");
  process.exit(0);
}

// 1. prune .mdx twins
let pruned = 0;
(function walk(dir: string) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) walk(p);
    else if (e.name === "index.mdx") { fs.unlinkSync(p); pruned++; }
  }
})(outDir);
console.log(`postbuild: pruned ${pruned} .mdx twins`);

// 2. stray URL rebase
execFileSync(process.execPath, [path.join(root, "bin/postbuild-rebase.ts"), outDir], { stdio: "inherit" });

// 4. nest under /docs (everything except __redirects and .assetsignore)
fs.mkdirSync(nested, { recursive: true });
for (const name of fs.readdirSync(outDir)) {
  if (name === "docs" || name === "__redirects" || name === ".assetsignore") continue;
  fs.renameSync(path.join(outDir, name), path.join(nested, name));
}
// 3. redirects at the top level (imported by the Worker; not served as an asset)
execFileSync(process.execPath, [path.join(root, "bin/gen-redirects.ts"), outDir], { stdio: "inherit" });
fs.copyFileSync(path.join(root, ".assetsignore"), path.join(outDir, ".assetsignore"));

// Workers static assets: 25 MiB per file, 100k files per version.
const LIMIT = 25 * 1024 * 1024;
const oversized: string[] = [];
(function scan(dir: string) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) scan(p);
    else if (fs.statSync(p).size > LIMIT) oversized.push(`${path.relative(outDir, p)} (${(fs.statSync(p).size / 1048576).toFixed(1)} MB)`);
  }
})(outDir);
if (oversized.length) {
  console.error(`postbuild: ${oversized.length} file(s) exceed the 25 MiB Workers asset limit:\n  ${oversized.join("\n  ")}`);
  process.exitCode = 1;
}

const files = (function count(dir: string): number {
  let n = 0;
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) n += e.isDirectory() ? count(path.join(dir, e.name)) : 1;
  return n;
})(outDir);
console.log(`postbuild: site nested under ${path.relative(root, nested)}; ${files} files total (Workers static-asset limit: 100,000)`);
