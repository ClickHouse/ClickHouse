// Builds a PascalCase component name -> import specifier index from how the
// content imports its components (`import X from "/snippets/..."`). Partial
// MDX files (underscore files, snippet MDX) use components without importing
// them because Mintlify inlined them into the importing page; the Vite plugin
// uses this index to inject the missing imports. Output:
// src/generated/import-index.json  { "SystemLogParameters": "/snippets/_system-log-parameters.mdx", ... }
import fs from "node:fs";
import path from "node:path";

const root = process.cwd();
const SECTIONS = ["get-started", "concepts", "guides", "reference", "products", "clickstack", "integrations", "resources", "chdb", "snippets"];
const counts = new Map<string, Map<string, number>>();

function bump(name: string, spec: string) {
  if (!/^[A-Z][A-Za-z0-9_]*$/.test(name)) return;
  const m = counts.get(name) ?? new Map<string, number>();
  m.set(spec, (m.get(spec) ?? 0) + 1);
  counts.set(name, m);
}

(function walk(dir: string) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    if (e.name === "node_modules") continue;
    const p = path.join(dir, e.name);
    if (e.isDirectory()) walk(p);
    else if (/\.mdx?$/.test(e.name)) {
      const src = fs.readFileSync(p, "utf8");
      for (const m of src.matchAll(/^import\s+(.+?)\s+from\s+["']([^"']+)["'];?\s*$/gm)) {
        const clause = m[1].trim();
        const spec = m[2];
        // Only site-absolute specifiers are location-independent.
        if (!spec.startsWith("/snippets/")) continue;
        const def = clause.match(/^([A-Za-z0-9_]+)(?:\s*,|$)/);
        if (def) bump(def[1], spec);
        const named = clause.match(/\{([^}]*)\}/);
        if (named) for (const part of named[1].split(",")) {
          const n = part.trim().split(/\s+as\s+/).pop()!.trim();
          if (n) bump(n, `${spec}#${part.trim().split(/\s+as\s+/)[0].trim()}`);
        }
      }
    }
  }
})(root);
for (const s of SECTIONS) if (!fs.existsSync(path.join(root, s))) throw new Error(`missing ${s}`);

const index: Record<string, string> = {};
for (const [name, m] of counts) {
  // Most frequently used specifier wins; English (non-locale) paths preferred.
  const best = [...m.entries()]
    .sort((a, b) => Number(/^\/snippets\/(ar|es|fr|ja|ko|pt-BR|ru|zh)\//.test(a[0])) - Number(/^\/snippets\/(ar|es|fr|ja|ko|pt-BR|ru|zh)\//.test(b[0])) || b[1] - a[1])[0];
  index[name] = best[0];
}
fs.mkdirSync(path.join(root, "src/generated"), { recursive: true });
fs.writeFileSync(path.join(root, "src/generated/import-index.json"), JSON.stringify(index, null, 2) + "\n");
console.log(`gen-import-index: ${Object.keys(index).length} component names`);
