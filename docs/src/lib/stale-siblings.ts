/**
 * A page file `X.mdx` next to a folder index `X/index.mdx` would collapse to
 * the same id `X` (both are served by Mintlify, at `/X` and `/X/index`). The
 * folder index is the live page in every such case (it is what navigation
 * references; English only has the folder), the sibling being a translation
 * artifact from before the page moved. Those siblings are excluded from the
 * collection instead of silently overwriting the index.
 */
import fs from "node:fs";
import path from "node:path";

export function staleSiblingPages(baseDir: string, sections: string[]): string[] {
  const out: string[] = [];
  for (const section of sections) {
    const dir = path.join(baseDir, section);
    if (!fs.existsSync(dir)) continue;
    (function walk(d: string) {
      for (const e of fs.readdirSync(d, { withFileTypes: true })) {
        if (e.name.startsWith("_")) continue;
        const p = path.join(d, e.name);
        if (e.isDirectory()) {
          walk(p);
          const sibling = `${p}.mdx`;
          if (fs.existsSync(path.join(p, "index.mdx")) && fs.existsSync(sibling)) {
            out.push(path.relative(baseDir, sibling).replaceAll(path.sep, "/"));
          }
        }
      }
    })(dir);
  }
  return out;
}
