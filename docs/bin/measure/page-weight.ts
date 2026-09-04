// HTML page weight distribution for a build. Usage: node bin/measure/page-weight.ts [dist]
import fs from "node:fs";
import path from "node:path";

const dist = path.resolve(process.cwd(), process.argv[2] ?? "dist");
const pages: Array<{ file: string; size: number; sidebar: number; drawer: number; links: number }> = [];
(function walk(dir: string) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) { if (e.name !== "nav") walk(p); }
    else if (e.name === "index.html") {
      const html = fs.readFileSync(p, "utf8");
      const sidebar = html.match(/<aside id="desktop-sidebar"[\s\S]*?<\/aside>/)?.[0].length ?? 0;
      const drawer = html.match(/<dialog data-mobile-sidebar[\s\S]*?<\/dialog>/)?.[0].length ?? 0;
      const links = (html.match(/data-nb-sidebar-link/g) ?? []).length;
      pages.push({ file: path.relative(dist, p), size: html.length, sidebar, drawer, links });
    }
  }
})(dist);
pages.sort((a, b) => a.size - b.size);
const q = (f: number) => pages[Math.min(pages.length - 1, Math.floor(f * pages.length))]?.size ?? 0;
const kb = (n: number) => `${(n / 1024).toFixed(0)} KB`;
const total = pages.reduce((n, p) => n + p.size, 0);
console.log(`page-weight: ${pages.length} pages, total ${(total / 1048576).toFixed(1)} MB, min ${kb(pages[0]?.size ?? 0)}, median ${kb(q(0.5))}, p95 ${kb(q(0.95))}, max ${kb(pages.at(-1)?.size ?? 0)}`);
const avgSidebar = pages.reduce((n, p) => n + p.sidebar + p.drawer, 0) / Math.max(1, pages.length);
const avgLinks = pages.reduce((n, p) => n + p.links, 0) / Math.max(1, pages.length);
console.log(`  sidebar+drawer average ${kb(avgSidebar)} (${(100 * avgSidebar * pages.length / total).toFixed(0)}% of bytes), average sidebar links ${avgLinks.toFixed(0)}`);
console.log("  largest pages:");
for (const p of pages.slice(-8).reverse()) console.log(`    ${kb(p.size)}  ${p.file}  (sidebar ${kb(p.sidebar + p.drawer)}, links ${p.links})`);
const nav = path.join(dist, "nav");
if (fs.existsSync(nav)) {
  let n = 0, bytes = 0;
  (function walk(dir: string) { for (const e of fs.readdirSync(dir, { withFileTypes: true })) { const p = path.join(dir, e.name); if (e.isDirectory()) walk(p); else { n++; bytes += fs.statSync(p).size; } } })(nav);
  console.log(`  lazy fragments: ${n} files, ${(bytes / 1048576).toFixed(1)} MB`);
}
