// Final safety net for the base path: rewrites site-root-relative URL
// attributes in built HTML that are not under /docs (values that came from
// JS data objects and never passed through the hast rebaser).
// Usage: node bin/postbuild-rebase.ts [dist]
import fs from "node:fs";
import path from "node:path";

const dist = path.resolve(process.cwd(), process.argv[2] ?? "dist");
const base = "/docs";
let files = 0, rewrites = 0;
(function walk(dir: string) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) walk(p);
    else if (e.name.endsWith(".html")) {
      const html = fs.readFileSync(p, "utf8");
      let n = 0;
      const out = html.replace(/\s(href|src|poster|srcset)="(\/[^"/][^"]*)"/g, (m, attr, url) => {
        if (url === base || url.startsWith(base + "/") || url.startsWith(base + "#") || url.startsWith(base + "?")) return m;
        n++;
        return ` ${attr}="${base}${url}"`;
      });
      if (n) { fs.writeFileSync(p, out); files++; rewrites += n; }
    }
  }
})(dist);
console.log(`postbuild-rebase: ${rewrites} URLs rewritten in ${files} files`);
