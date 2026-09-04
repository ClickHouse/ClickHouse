// Compares the live Mintlify sitemap with the built site: every sitemap URL
// must resolve to a built page or a redirect source.
// Usage: node bin/measure/url-parity.ts [dist] [--sitemap tmp/mintlify-sitemap.xml] [--locales]
import fs from "node:fs";
import path from "node:path";

const args = process.argv.slice(2);
const dist = path.resolve(process.cwd(), args.find((a) => !a.startsWith("--")) ?? "dist");
const sitemapArg = args.indexOf("--sitemap");
const sitemapFile = sitemapArg >= 0 ? args[sitemapArg + 1] : "tmp/mintlify-sitemap.xml";
const includeLocales = args.includes("--locales");
const base = "/docs";
// Mintlify lowercases URL segments: the pt-BR tree is served at /pt-br/.
const LOCALES = ["ar", "es", "fr", "ja", "ko", "pt-br", "ru", "zh"];

let xml: string;
if (fs.existsSync(sitemapFile)) xml = fs.readFileSync(sitemapFile, "utf8");
else {
  const res = await fetch("https://clickhouse.com/docs/sitemap.xml", { headers: { "user-agent": "Mozilla/5.0" } });
  xml = await res.text();
  fs.mkdirSync(path.dirname(sitemapFile), { recursive: true });
  fs.writeFileSync(sitemapFile, xml);
}
const norm = (p: string) => p.replace(/\/+$/, "") || "/";
const sitemapPaths = [...xml.matchAll(/<loc>([^<]+)<\/loc>/g)]
  .map((m) => new URL(m[1].trim()).pathname)
  .filter((p) => p === base || p.startsWith(base + "/"))
  .map((p) => norm(p.slice(base.length) || "/"))
  .map((p) => p.toLowerCase())
  .filter((p) => includeLocales || !LOCALES.some((l) => p === `/${l}` || p.startsWith(`/${l}/`)));

const built = new Set<string>();
(function walk(dir: string) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) walk(p);
    else if (e.name === "index.html") built.add(norm("/" + path.relative(dist, path.dirname(p)).replaceAll(path.sep, "/")).toLowerCase());
  }
})(dist);

const redirects = JSON.parse(fs.readFileSync("_site/redirects.json", "utf8")) as Array<{ source: string; destination: string }>;
const redirectSources = new Set(redirects.map((r) => norm(r.source.replace(/^\/docs(?=\/|$)/, "").split("#")[0]).toLowerCase()));

let resolved = 0;
const missing: string[] = [];
for (const p of sitemapPaths) {
  if (built.has(p) || redirectSources.has(p)) resolved++;
  else missing.push(p);
}
const bySection = new Map<string, number>();
for (const m of missing) bySection.set(m.split("/")[1] ?? "/", (bySection.get(m.split("/")[1] ?? "/") ?? 0) + 1);

console.log(`url-parity: ${resolved}/${sitemapPaths.length} sitemap URLs resolve (${((100 * resolved) / Math.max(1, sitemapPaths.length)).toFixed(2)}%); built pages: ${built.size}; redirect sources: ${redirectSources.size}`);
for (const [s, n] of [...bySection.entries()].sort((a, b) => b[1] - a[1])) console.log(`  missing in /${s}: ${n}`);
for (const m of missing.slice(0, 40)) console.log(`  - ${m}`);
if (missing.length > 40) console.log(`  ... ${missing.length - 40} more`);
fs.mkdirSync("reports", { recursive: true });
fs.writeFileSync("reports/url-parity-missing.txt", missing.join("\n") + "\n");
process.exitCode = missing.length ? 1 : 0;
