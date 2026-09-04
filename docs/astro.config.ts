import { defineConfig } from "astro/config";
import react from "@astrojs/react";
import tailwindcss from "@tailwindcss/vite";
import nimbus, { defineConfig as defineNimbusConfig } from "@cloudflare/nimbus-docs";
import { tableScroll } from "@cloudflare/nimbus-docs/markdown";
import { satteri } from "@astrojs/markdown-satteri";
import { rebaseUrls } from "./src/plugins/satteri-rebase-urls";
import { mermaidBlocks } from "./src/plugins/satteri-mermaid";
import { SATTERI_FEATURES } from "./src/plugins/satteri-features";
import fs from "node:fs";

// Sidebar generated from docs.json + navigation.json by bin/gen-sidebar.ts
// (prebuild). Falls back to Nimbus's filesystem sidebar when absent.
const sidebarItemsFile = new URL("./src/generated/sidebar.items.json", import.meta.url);
const sidebarItems = fs.existsSync(sidebarItemsFile)
  ? (JSON.parse(fs.readFileSync(sidebarItemsFile, "utf8")) as NonNullable<Parameters<typeof defineNimbusConfig>[0]["sidebar"]>["items"])
  : undefined;
import { mintlifySnippets } from "./src/plugins/vite-mintlify-snippets";
import { clickhouseSqlTransformer } from "./src/plugins/shiki-clickhouse-sql";

// The site is served at clickhouse.com/docs behind the website Worker.
export const BASE = "/docs";

const nimbusConfig = defineNimbusConfig({
  site: "https://clickhouse.com",
  title: "ClickHouse Docs",
  description: "Documentation for ClickHouse, the fastest open-source analytical database.",
  locale: "en",
  github: "https://github.com/ClickHouse/ClickHouse",
  // `{path}` is the entry's repo-relative file path (e.g. reference/x.mdx).
  editPattern: "https://github.com/ClickHouse/ClickHouse/edit/master/docs/{path}",
  socialImageAlt: "ClickHouse documentation",
  // Inkeep provides search; no Pagefind index is built.
  search: { provider: "custom" },
  sidebar: {
    items: sidebarItems,
    scope: "section",
    defaultCollapsed: true,
  },
});

export default defineConfig({
  site: "https://clickhouse.com",
  base: BASE,
  output: "static",
  // Overridable so parallel builds (CI shards, concurrent sessions) never
  // share an output directory or the content-layer cache.
  outDir: process.env.DOCS_OUT_DIR ?? "./dist",
  cacheDir: process.env.DOCS_CACHE_DIR ?? "./node_modules/.astro",
  trailingSlash: "ignore",
  prefetch: { prefetchAll: true, defaultStrategy: "hover" },
  markdown: {
    // Mermaid fences are rendered client-side (src/plugins/satteri-mermaid.ts).
    syntaxHighlight: { type: "shiki", excludeLangs: ["mermaid"] },
    shikiConfig: {
      // Non-standard fence languages used by the generated reference docs.
      langAlias: {
        response: "text", result: "text", results: "text", test: "text", code: "text",
        SQL: "sql", Python: "python", python3: "python", capnp: "text",
      },
      transformers: [clickhouseSqlTransformer()],
    },
  },
  integrations: [
    react(),
    nimbus(nimbusConfig, {
      // Mintlify-named components are provided by src/components.ts; the
      // PascalCase pre-check does not know about MDX snippets imported as
      // components, so it is disabled and the build itself is the validator.
      validateMdx: false,
      markdown: {
        // Own processor so that `## Heading {#custom-id}` is honoured
        // (Sätteri's headingAttributes is off by default and not exposed by
        // Nimbus). Nimbus's own hast plugins must be re-added here.
        processor: satteri({
          features: SATTERI_FEATURES,
          hastPlugins: [tableScroll(), rebaseUrls({ base: BASE }), mermaidBlocks()],
        }),
      },
    }),
  ],
  vite: {
    // The Vite dependency optimizer currently resolves React's development
    // JSX runtime to its production implementation in this project. The
    // production runtime intentionally leaves `jsxDEV` undefined, which made
    // the generated homepage React islands fail only after client hydration.
    // Use the automatic runtime's `jsx` helper for source JSX in both modes.
    esbuild: {
      jsx: "automatic",
      jsxImportSource: "react",
      jsxDev: false,
    },
    plugins: [tailwindcss(), mintlifySnippets()],
    resolve: { dedupe: ["react", "react-dom"] },
  },
});
