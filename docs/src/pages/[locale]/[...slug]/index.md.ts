/**
 * Markdown twins for locale pages: `/<locale>/<slug>/index.md`. Untranslated
 * (fallback) pages have no twin of their own; the English twin is canonical.
 */
import { cleanMarkdown, entriesFor, type IndexedEntry } from "../../../lib/corpus";
import { config } from "virtual:nimbus/config";
import { withBase } from "../../../lib/base";
import { ACTIVE_LOCALES, localeCollectionName } from "../../../content.config";

export const prerender = true;

interface Props {
  item: IndexedEntry;
}

export async function getStaticPaths() {
  const paths: Array<{ params: { locale: string; slug: string }; props: Props }> = [];
  for (const locale of ACTIVE_LOCALES.map(localeCollectionName)) {
    for (const item of await entriesFor(locale)) {
      paths.push({ params: { locale, slug: item.entry.id }, props: { item } });
    }
  }
  return paths;
}

export async function GET({ props, params }: { props: Props; params: { locale: string } }) {
  const { item } = props;
  const { entry, title, description } = item;
  const markdown = cleanMarkdown(entry);
  const body = [
    "---",
    `title: ${JSON.stringify(title)}`,
    ...(description ? [`description: ${JSON.stringify(description)}`] : []),
    `lang: ${JSON.stringify(params.locale)}`,
    "---",
    "",
    "> Documentation Index",
    `> Fetch the complete documentation index at: ${new URL(withBase(`/${params.locale}/llms.txt`), config.site).href}`,
    "> Use this file to discover all available pages before exploring further.",
    "",
    `# ${title}`,
    "",
    markdown,
    "",
    `Source: ${new URL(withBase(`/${params.locale}/${entry.id}/`), config.site).href}`,
    "",
  ].join("\n");
  return new Response(body, { headers: { "Content-Type": "text/markdown; charset=utf-8" } });
}
