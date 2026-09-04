/**
 * Per-locale agent index: `/<locale>/llms.txt` lists the translated pages of
 * that locale with links to their markdown twins.
 */
import { entriesFor } from "../../lib/corpus";
import { config } from "virtual:nimbus/config";
import { withBase } from "../../lib/base";
import { ACTIVE_LOCALES, localeCollectionName } from "../../content.config";

export const prerender = true;

export function getStaticPaths() {
  return ACTIVE_LOCALES.map(localeCollectionName).map((locale) => ({ params: { locale } }));
}

export async function GET({ params }: { params: { locale: string } }) {
  const locale = params.locale;
  const items = await entriesFor(locale);
  items.sort((a, b) => a.entry.id.localeCompare(b.entry.id));
  const lines = [
    `# ${config.title} (${locale})`,
    "",
    config.description ?? "",
    "",
    `English index: ${new URL(withBase("/llms.txt"), config.site).href}`,
    "",
    "## Pages",
    "",
    ...items.map((i) => {
      const url = new URL(withBase(`/${locale}/${i.entry.id}/index.md`), config.site).href;
      return `- [${i.title}](${url})${i.description ? ` — ${i.description}` : ""}`;
    }),
    "",
  ];
  return new Response(lines.join("\n"), { headers: { "Content-Type": "text/plain; charset=utf-8" } });
}
