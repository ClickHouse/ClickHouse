import fs from "node:fs";
import path from "node:path";
import type { Plugin } from "vite";

/**
 * Makes the existing Mintlify content build unchanged:
 *  - resolves `/snippets/**` imports to `docs/snippets/**`, accepting the
 *    `.js` specifiers Mintlify uses for `.jsx` files;
 *  - injects React hook imports and Mintlify built-in components into the
 *    snippet JSX components, which rely on Mintlify's implicit globals;
 *  - injects the Mintlify built-in components into snippet MDX files that
 *    are imported as components (they only import their custom components).
 */

const HOOKS = ["useState", "useEffect", "useRef", "useMemo", "useCallback", "useLayoutEffect", "useId"];
const REACT_GLOBALS = ["Frame", "Icon", "Card", "Accordion", "Tooltip", "Banner", "Expandable", "Badge"];
const MDX_GLOBALS = [
  "Note", "Info", "Tip", "Warning", "Danger", "Check",
  "Tabs", "Tab", "Steps", "Step", "Accordion", "AccordionGroup",
  "Card", "CardGroup", "Frame", "Badge", "Tooltip", "Columns", "Update", "Icon", "Expandable", "View", "Visibility", "CodeBlock",
];
const REACT_ISLAND_QUERY = "?clickhouse-react-island";

// Mintlify injected these identifiers for old knowledge-base diagrams. The
// files are already public assets in this repository, so preserve the source
// MDX and turn only these unresolved expressions into their public URLs.
const LEGACY_IMAGE_VARIABLES: Record<string, string> = {
  sync_read: "/images/knowledgebase/sync_read.webp",
  async_read: "/images/knowledgebase/async_read.webp",
  optimize_read: "/images/knowledgebase/optimize_read.webp",
  ad_env: "/images/knowledgebase/windows-ad-ch-roles/AD_Env_and_UO_structure.webp",
  ad_group: "/images/knowledgebase/windows-ad-ch-roles/AD_Group_clickhouse_ad_db1_users.webp",
  ad_user: "/images/knowledgebase/windows-ad-ch-roles/AD_user_clickhouse_db1_user.webp",
  ad_user_group: "/images/knowledgebase/windows-ad-ch-roles/AD_user_to_group.webp",
};

export function mintlifySnippets(): Plugin {
  let root = process.cwd();
  const snippetsDir = () => path.join(root, "snippets");
  const virtualMdxPrefix = "\0clickhouse:snippet-mdx:";
  let interactive = new Set<string>();
  let importIndex: Record<string, string> = {};

  return {
    name: "clickhouse:mintlify-snippets",
    enforce: "pre",
    configResolved(config) {
      root = config.root;
      const manifest = path.join(root, "src/compat/interactive-components.json");
      interactive = new Set(JSON.parse(fs.readFileSync(manifest, "utf8")).components as string[]);
      const indexFile = path.join(root, "src/generated/import-index.json");
      importIndex = fs.existsSync(indexFile) ? (JSON.parse(fs.readFileSync(indexFile, "utf8")) as Record<string, string>) : {};
    },
    resolveId(source) {
      if (!source.startsWith("/snippets/")) return null;
      // Generated Astro wrappers import the underlying React implementation
      // with this query. Keep that browser request on the JSX source instead
      // of redirecting it back to the Astro wrapper below.
      if (source.endsWith(REACT_ISLAND_QUERY)) {
        const directSource = source.slice(0, -REACT_ISLAND_QUERY.length);
        const target = path.join(root, directSource.slice(1));
        if (!fs.existsSync(target)) throw new Error(`mintlifySnippets: ${target} not found`);
        return target + REACT_ISLAND_QUERY;
      }
      // Interactive components hydrate through generated island wrappers.
      const m = source.match(/^\/snippets\/(?:([A-Za-z-]+)\/)?components\/([A-Za-z0-9_]+)\/\2\.jsx?$/);
      if (m && interactive.has(m[2])) {
        const wrapper = path.join(root, "src/generated/compat-wrappers", m[1] ?? "", `${m[2]}.ts`);
        if (fs.existsSync(wrapper)) return wrapper;
      }
      const target = path.join(root, source.slice(1));
      const candidates = [target];
      if (target.endsWith(".js")) candidates.push(target.slice(0, -3) + ".jsx", target.slice(0, -3) + ".tsx");
      if (!path.extname(target)) candidates.push(target + ".jsx", target + ".mdx", target + ".tsx");
      for (const c of candidates) {
        if (!fs.existsSync(c)) continue;
        // Astro's content compiler reads physical `.mdx` files before Vite's
        // `transform` hook runs. Returning a virtual module makes `load` the
        // authoritative source and lets us add the imports Mintlify provided
        // implicitly to nested snippets.
        if (c.endsWith(".mdx")) return virtualMdxPrefix + c;
        return c;
      }
      return null;
    },
    load(id) {
      if (!id.startsWith(virtualMdxPrefix)) return null;
      const file = id.slice(virtualMdxPrefix.length);
      return normalizeMdx(fs.readFileSync(file, "utf8"), importIndex);
    },
    transform(code, id) {
      const file = id.split("?")[0];
      if (!file.startsWith(root + path.sep) || file.includes(`${path.sep}node_modules${path.sep}`)) return null;

      if (file.endsWith(".jsx") && file.startsWith(snippetsDir())) return { code: injectReact(code), map: null };
      if (file.endsWith(".mdx")) return { code: normalizeMdx(code, importIndex), map: null };
      return null;
    },
  };
}

/**
 * Whether `name` is bound by an import statement or a top-level declaration.
 * Anchored to line starts and single statements: prose such as "import the
 * data ... from" must never count as a declaration.
 */
export function declared(code: string, name: string): boolean {
  const singleLineImport = new RegExp(`^import\\s[^\\n]*\\b${name}\\b[^\\n]*\\sfrom\\s`, "m");
  const bracedImport = new RegExp(`^import\\s*(?:[A-Za-z0-9_$]+\\s*,\\s*)?\\{[^}]*\\b${name}\\b[^}]*\\}\\s*from\\s`, "m");
  const declaration = new RegExp(`^(?:export\\s+)?(?:const|let|var|function|class)\\s+${name}\\b`, "m");
  return singleLineImport.test(code) || bracedImport.test(code) || declaration.test(code);
}

export function injectReact(code: string): string {
  // Image paths in JS data (`img: "/images/x.webp"`) never pass through the
  // hast rebaser; prefix them here so SSR and hydration agree.
  code = code.replace(/(["'`])\/images\//g, "$1/docs/images/");
  const hooks = HOOKS.filter((h) => new RegExp(`\\b${h}\\s*\\(`).test(code) && !declared(code, h));
  const globals = REACT_GLOBALS.filter((g) => new RegExp(`<${g}[\\s/>]`).test(code) && !declared(code, g));
  const lines: string[] = [];
  if (hooks.length) lines.push(`import { ${hooks.join(", ")} } from "react";`);
  if (globals.length) lines.push(`import { ${globals.join(", ")} } from "/src/components/compat/react/mintlify-react.tsx";`);
  return lines.length ? lines.join("\n") + "\n" + code : code;
}

/** Identifiers Astro's MDX wrapper declares itself; user imports must not reuse them. */
const RESERVED = ["Content", "components", "frontmatter", "MDXContent"];

/**
 * Makes Mintlify-flavoured MDX acceptable to Sätteri/mdxjs-rs:
 *  - undeclared Mintlify built-ins -> compat globals import;
 *  - undeclared custom components -> import from the content-wide import index
 *    (Mintlify inlined snippets into pages, so partials never imported them);
 *  - `import Content ...` and similar reserved identifiers are renamed;
 *  - an import block glued to the following content gets a blank line
 *    (otherwise the next line is parsed as ESM).
 */
export function normalizeMdx(code: string, importIndex: Record<string, string>): string {
  let out = code;

  out = out.replace(/(<Image\b[^>]*\bimg=)\{([A-Za-z_][A-Za-z0-9_]*)\}/g, (match, prefix, name) => {
    const image = LEGACY_IMAGE_VARIABLES[name];
    return image && !declared(out, name) ? `${prefix}${JSON.stringify(image)}` : match;
  });

  // Reserved identifiers.
  for (const name of RESERVED) {
    const re = new RegExp(`^import\\s+${name}\\b`, "m");
    if (!re.test(out)) continue;
    const renamed = `${name}_`;
    out = out.replace(new RegExp(`^(import\\s+)${name}\\b`, "gm"), `$1${renamed}`);
    out = out.replace(new RegExp(`<(\\/?)${name}(?=[\\s/>])`, "g"), `<$1${renamed}`);
  }

  // Blank line after an import block that runs straight into content.
  out = out.replace(/^((?:import\s[^\n]*\n)+)(?!import\s|export\s|\n|\r)/gm, "$1\n");

  // mdxjs-rs ends an ESM block at a blank line (micromark keeps parsing while
  // the program is incomplete), so `export const X = (<>...</>)` components
  // with blank lines inside break. Drop blank lines while a block is open.
  out = stripBlankLinesInEsm(out);

  // React hooks inside inline `export const` components: render-once shims
  // (MDX-defined components run in Astro's JSX runtime, not React).
  const hooksUsed = HOOKS.filter((h) => new RegExp(`\\b${h}\\s*\\(`).test(out) && !declared(out, h));
  if (hooksUsed.length && !/from\s+["']react["']/.test(out)) {
    out = injectAfterFrontmatter(out, `import { ${hooksUsed.join(", ")} } from "/src/components/compat/react/hook-shims.ts";\n`);
  }

  // Missing component imports.
  const tags = new Set([...out.matchAll(/<([A-Z][A-Za-z0-9_]*)[\s/>]/g)].map((m) => m[1]));
  const globals: string[] = [];
  const custom: string[] = [];
  for (const tag of tags) {
    if (declared(out, tag)) continue;
    if (MDX_GLOBALS.includes(tag)) globals.push(tag);
    else if (importIndex[tag]) custom.push(tag);
  }
  const lines: string[] = [];
  if (globals.length) lines.push(`import { ${globals.join(", ")} } from "/src/components/compat/globals.ts";`);
  for (const tag of custom) {
    const spec = importIndex[tag];
    const [file, named] = spec.split("#");
    lines.push(named && named !== tag ? `import { ${named} as ${tag} } from "${file}";` : named ? `import { ${tag} } from "${file}";` : `import ${tag} from "${file}";`);
  }
  if (!lines.length) return out;
  return injectAfterFrontmatter(out, lines.join("\n") + "\n\n");
}

/** MDX imports must follow the frontmatter block, if any. */
function injectAfterFrontmatter(code: string, block: string): string {
  const fm = code.match(/^---\r?\n[\s\S]*?\r?\n---\r?\n/);
  return fm ? fm[0] + block + code.slice(fm[0].length) : block + code;
}

function stripBlankLinesInEsm(code: string): string {
  const lines = code.split("\n");
  const out: string[] = [];
  let inFence = false;
  let inEsm = false;
  let depth = 0;
  for (const line of lines) {
    if (!inEsm && /^(```|~~~)/.test(line)) inFence = !inFence;
    if (inFence) { out.push(line); continue; }
    if (!inEsm && /^(export|import)\s/.test(line)) { inEsm = true; depth = 0; }
    if (inEsm) {
      if (line.trim() === "") continue; // the blank line that would end the block
      for (const ch of line) {
        if (ch === "(" || ch === "{" || ch === "[") depth++;
        else if (ch === ")" || ch === "}" || ch === "]") depth--;
      }
      out.push(line);
      if (depth <= 0 && !/[,(\[{=]\s*$/.test(line)) { inEsm = false; depth = 0; }
      continue;
    }
    out.push(line);
  }
  return out.join("\n");
}
