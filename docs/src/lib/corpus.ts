/**
 * Chunked agent corpora. A single `llms-full.txt` for the whole site exceeds
 * the 25 MiB Workers asset limit (26.3 MB for English alone), so the corpus is
 * emitted per top-level section (and per locale), and the root file is an index.
 */
import { renderEntryAsMarkdown } from "@cloudflare/nimbus-docs";
import { getCollection, type CollectionEntry, type CollectionKey } from "astro:content";
import fs from "node:fs";
import path from "node:path";
import { config } from "virtual:nimbus/config";
import { withBase } from "./base";

export const PRIMARY = "docs";

/** What the agent surfaces need per page (a subset of Nimbus's IndexedEntry). */
export interface IndexedEntry {
  entry: CollectionEntry<CollectionKey>;
  collection: string;
  title: string;
  description: string | undefined;
  /** Site-relative page URL without base, trailing slash. */
  url: string;
}

export function sectionOf(entry: IndexedEntry): string {
  return entry.entry.id.split("/")[0] ?? "";
}

/**
 * Pages of one collection. Read straight from Astro: Nimbus's own index only
 * covers collections declared statically in content.config.ts, and the locale
 * collections are generated from the build scope.
 */
export async function entriesFor(collection: string): Promise<IndexedEntry[]> {
  const entries = (await getCollection(collection as CollectionKey)) as CollectionEntry<CollectionKey>[];
  const prefix = collection === PRIMARY ? "" : `/${collection}`;
  return entries
    .filter((e) => e.id !== "index" && !(e.data as { draft?: boolean }).draft)
    .map((e) => {
      const d = e.data as { title?: string; sidebarTitle?: string; description?: string };
      return {
        entry: e,
        collection,
        title: d.title ?? d.sidebarTitle ?? e.id.split("/").pop() ?? e.id,
        description: d.description || undefined,
        url: `${prefix}/${e.id}/`,
      };
    });
}

export function sectionsOf(entries: IndexedEntry[]): string[] {
  return [...new Set(entries.map(sectionOf))].sort();
}

/** Nimbus's markdown rendering, minus the ESM imports and MDX comments of the source. */
function expandAgentSnippetImports(body: string): string {
  const imports = new Map<string, string>();
  for (const match of body.matchAll(/^import\s+([A-Z][A-Za-z0-9_]*)\s+from\s+["'](\/[^"']+\.mdx)["'];?\s*$/gm)) {
    imports.set(match[1], match[2]);
  }

  return body.replace(/<Visibility\b([^>]*)\bfor=["']agents["']([^>]*)>([\s\S]*?)<\/Visibility>/g, (_block, before, after, children) => {
    let expanded = children as string;
    for (const [name, source] of imports) {
      const component = new RegExp(`<${name}\\s*\\/>`, "g");
      if (!component.test(expanded)) continue;
      const file = path.join(process.cwd(), source.slice(1));
      const snippet = fs.readFileSync(file, "utf8").replace(/^---\n[\s\S]*?\n---\n?/, "");
      expanded = expanded.replace(component, snippet);
    }
    return `<Visibility${before} for="agents"${after}>${expanded}</Visibility>`;
  });
}

/** Markdown twins expose agent-only content and exclude web-only content. */
export function cleanMarkdown(entry: IndexedEntry["entry"]): string {
  const body = expandAgentSnippetImports(entry.body ?? "");
  return renderEntryAsMarkdown(
    { body },
    {
      componentMap: {
        Visibility: ({ attrs, children }) => attrs.for === "agents" ? children : "",
        View: ({ attrs, children }) => typeof attrs.title === "string" ? `**${attrs.title}**\n\n${children}` : children,
      },
    },
  )
    .replace(/\{\/\*[\s\S]*?\*\/\}/g, "")
    .split("\n")
    .filter((line) => !/^import\s.+\sfrom\s+["'][^"']+["'];?\s*$/.test(line))
    .join("\n")
    .replace(/\n{3,}/g, "\n\n");
}

/** One `#`-block per page, sorted by URL; deterministic across rebuilds. */
export function renderCorpus(entries: IndexedEntry[], title: string, indexPath: string): string {
  const sorted = [...entries].sort((a, b) => a.url.localeCompare(b.url));
  const blocks = sorted.map((i) => {
    const url = new URL(withBase(i.url), config.site).href;
    return `# ${i.title}\n\nSource: ${url}\n\n${cleanMarkdown(i.entry)}\n`;
  });
  return [
    `# ${title}`,
    "",
    `> Full-text corpus of one documentation section. Index: ${new URL(withBase(indexPath), config.site).href}`,
    "",
    ...blocks,
  ].join("\n");
}

export function renderCorpusIndex(title: string, sections: string[], prefix: string): string {
  const lines = [
    `# ${title}: full-text corpus`,
    "",
    "> The corpus is split by section so each file stays below 25 MiB. Fetch the sections you need:",
    "",
    ...sections.map((s) => `- ${new URL(withBase(`${prefix}/${s}/llms-full.txt`), config.site).href}`),
    "",
    `Page index: ${new URL(withBase(`${prefix}/llms.txt`), config.site).href}`,
    "",
  ];
  return lines.join("\n");
}
