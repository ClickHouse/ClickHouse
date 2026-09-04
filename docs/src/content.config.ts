import { defineCollection } from "astro:content";
import { glob } from "astro/loaders";
import { z } from "astro/zod";
import { defineDocSchema } from "@cloudflare/nimbus-docs/schemas";
import { readScope } from "./lib/scope";
import { staleSiblingPages } from "./lib/stale-siblings";

/** Top-level English content directories (the Mintlify tabs' sources). */
export const SECTIONS = [
  "get-started",
  "concepts",
  "guides",
  "reference",
  "products",
  "clickstack",
  "integrations",
  "resources",
  "chdb",
];

/**
 * DOCS_INCLUDE=comma,separated,globs restricts the primary collection to a
 * slice of the tree (P0 spike, scoped previews).
 */
function splitPatterns(value: string): string[] {
  const patterns: string[] = [];
  let current = "";
  let braceDepth = 0;

  for (const character of value) {
    if (character === "{") braceDepth++;
    if (character === "}") braceDepth = Math.max(0, braceDepth - 1);

    if (character === "," && braceDepth === 0) {
      const pattern = current.trim();
      if (pattern) patterns.push(pattern);
      current = "";
      continue;
    }

    current += character;
  }

  const pattern = current.trim();
  if (pattern) patterns.push(pattern);
  return patterns;
}

const include = splitPatterns(process.env.DOCS_INCLUDE ?? "");

// Preview builds omit the reference section (`reference: false` in the scope):
// no clickhouse binary is needed and the build is a fraction of the size.
const scopedSections = SECTIONS.filter((s) => readScope().reference || s !== "reference");
const docsPattern = include.length
  ? include
  : [`{${scopedSections.join(",")}}/**/*.{md,mdx}`];
// The homepage (`index.mdx`, `mode: custom`, inline React with hooks) is not
// part of the collection: it is rebuilt natively as src/pages/index.astro.

/**
 * URL id = file path without extension, exactly as Mintlify does it. The
 * `slug` frontmatter key is stale Docusaurus data and is deliberately ignored
 * (Astro's default generateId would otherwise turn it into the URL).
 */
export function pathId({ entry }: { entry: string }): string {
  // Mintlify serves `folder/index.mdx` at both `/folder` and `/folder/index`;
  // its sitemap canonical is `/folder`, so that is the page id here and
  // `/folder/index` becomes a redirect (bin/gen-redirects.ts). The root
  // `index.mdx` keeps the id `index`, which Nimbus expects.
  const id = entry.replace(/\.(mdx?|md)$/i, "");
  return id === "index" ? id : id.replace(/\/index$/, "");
}

/** Frontmatter keys the current content uses that Nimbus's base schema lacks. */
const clickhouseFields = {
  // A few pages (the homepage, some landing pages) have no `title`; Nimbus
  // requires one. Made optional here; the route falls back to sidebarTitle/id.
  title: z.string().optional(),
  sidebarTitle: z.string().optional(),
  doc_type: z.string().optional(),
  keywords: z.union([z.string(), z.array(z.string())]).optional(),
  toc_max_heading_level: z.number().optional(),
  // Mintlify supports `wide`; Nimbus only `doc` | `custom`. Handled in the route.
  // Lenient: a few pages carry stray values.
  mode: z.any().optional().transform((v) => (v === "custom" || v === "wide" ? v : "doc")),
  // Docusaurus leftovers that Mintlify ignores but Nimbus would act on:
  // `sidebar: sqlreference` / `sidebar: false` (Nimbus: object | false) and
  // `draft: true` (Nimbus drops the page). Neutralised for URL parity.
  sidebar: z.any().optional().transform((v) => (v && typeof v === "object" ? v : undefined)),
  draft: z.any().optional().transform(() => false),
  slug: z.string().optional(),
  tag: z.string().optional(),
  openapi: z.string().optional(),
  integration: z.any().optional(),
  tags: z.any().optional(),
  date: z.any().optional(),
  show_related_blogs: z.any().optional(),
  rss: z.any().optional(),
  icon: z.string().optional(),
};

const schema = defineDocSchema({ fields: clickhouseFields, strictFrontmatter: false });

const changelogSchema = z.object({
  title: z.string(),
  description: z.string(),
  date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  products: z.array(z.string()).min(1),
  channel: z.string().optional(),
  hidden: z.boolean().default(false),
});

/** All translated trees; each is its own collection mounted at /<locale>. */
export const LOCALES = ["ar", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh"] as const;

/**
 * Locales come from the build scope (`DOCS_LOCALES` env, or the committed
 * `.preview-scope.json` on preview branches; see src/lib/scope.ts). Collection
 * names deliberately avoid the `docs-<x>` prefix, which Nimbus reserves for versions.
 */
const scope = readScope();
export const ACTIVE_LOCALES: string[] = [...scope.locales];

/**
 * Collection name = URL prefix. Mintlify lowercases URL segments, so the
 * `pt-BR` directory is served at `/pt-br/`; the collection is named accordingly.
 */
export const localeCollectionName = (locale: string) => locale.toLowerCase();

/** Patterns for one tree: the section globs minus stale `X.mdx` siblings of `X/index.mdx`. */
function treePattern(baseDir: string): string[] {
  return [...docsPattern, ...staleSiblingPages(baseDir, scopedSections).map((p) => `!${p}`)];
}

const localeCollections = Object.fromEntries(
  ACTIVE_LOCALES.map((locale) => [
    localeCollectionName(locale),
    defineCollection({
      loader: glob({ base: `./${locale}`, pattern: treePattern(`./${locale}`), generateId: pathId }),
      schema,
    }),
  ]),
);

export const collections = {
  docs: defineCollection({
    loader: glob({ base: ".", pattern: treePattern("."), generateId: pathId }),
    // Non-strict: the content carries Docusaurus-era keys we do not model.
    schema,
  }),
  changelog: defineCollection({
    loader: glob({ base: ".remote/changelog", pattern: "**/*.mdx", generateId: pathId }),
    schema: changelogSchema,
  }),
  ...localeCollections,
};
