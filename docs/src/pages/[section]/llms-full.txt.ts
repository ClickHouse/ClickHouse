// Per-section corpus for the English docs: /<section>/llms-full.txt
import { config } from "virtual:nimbus/config";
import { PRIMARY, entriesFor, renderCorpus, sectionOf, sectionsOf } from "../../lib/corpus";

export const prerender = true;

export async function getStaticPaths() {
  return sectionsOf(await entriesFor(PRIMARY)).map((section) => ({ params: { section } }));
}

export async function GET({ params }: { params: { section: string } }) {
  const entries = (await entriesFor(PRIMARY)).filter((i) => sectionOf(i) === params.section);
  return new Response(renderCorpus(entries, `${config.title} / ${params.section}`, "/llms.txt"), {
    headers: { "Content-Type": "text/plain; charset=utf-8" },
  });
}
