// Every site-relative href/src in the built HTML must live under the base
// path. Usage: node bin/measure/base-check.ts [dist]
import fs from "node:fs";
import path from "node:path";

const dist = path.resolve(process.cwd(), process.argv[2] ?? "dist");
const base = "/docs";

function* walk(dir: string): Generator<string> {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) yield* walk(p);
    else if (e.name.endsWith(".html")) yield p;
  }
}

const offenders = new Map<string, number>();
let total = 0;
for (const html of walk(dist)) {
  const out = fs.readFileSync(html, "utf8");
  for (const m of out.matchAll(/\s(?:href|src|poster)="(\/[^"]*)"/g)) {
    const url = m[1];
    total++;
    if (url.startsWith("//") || url === base || url.startsWith(base + "/") || url.startsWith(base + "#") || url.startsWith(base + "?")) continue;
    offenders.set(url, (offenders.get(url) ?? 0) + 1);
  }
}
const bad = [...offenders.entries()].sort((a, b) => b[1] - a[1]);
console.log(`base-check: ${total} site-relative URLs, ${bad.reduce((n, [, c]) => n + c, 0)} outside ${base}`);
for (const [url, c] of bad.slice(0, 25)) console.log(`  ${c}x ${url}`);
process.exitCode = bad.length ? 1 : 0;
