// Per-locale, per-section corpus: /<locale>/<section>/llms-full.txt
import { config } from "virtual:nimbus/config";
import { entriesFor, renderCorpus, sectionOf, sectionsOf } from "../../../lib/corpus";
import { ACTIVE_LOCALES, localeCollectionName } from "../../../content.config";

export const prerender = true;

export async function getStaticPaths() {
  const paths: Array<{ params: { locale: string; section: string } }> = [];
  for (const locale of ACTIVE_LOCALES.map(localeCollectionName)) {
    for (const section of sectionsOf(await entriesFor(locale))) paths.push({ params: { locale, section } });
  }
  return paths;
}

export async function GET({ params }: { params: { locale: string; section: string } }) {
  const entries = (await entriesFor(params.locale)).filter((i) => sectionOf(i) === params.section);
  return new Response(renderCorpus(entries, `${config.title} (${params.locale}) / ${params.section}`, `/${params.locale}/llms.txt`), {
    headers: { "Content-Type": "text/plain; charset=utf-8" },
  });
}
