// Per-locale corpus index: /<locale>/llms-full.txt
import { config } from "virtual:nimbus/config";
import { entriesFor, renderCorpusIndex, sectionsOf } from "../../lib/corpus";
import { ACTIVE_LOCALES, localeCollectionName } from "../../content.config";

export const prerender = true;

export function getStaticPaths() {
  return ACTIVE_LOCALES.map(localeCollectionName).map((locale) => ({ params: { locale } }));
}

export async function GET({ params }: { params: { locale: string } }) {
  const sections = sectionsOf(await entriesFor(params.locale));
  return new Response(renderCorpusIndex(`${config.title} (${params.locale})`, sections, `/${params.locale}`), {
    headers: { "Content-Type": "text/plain; charset=utf-8" },
  });
}
