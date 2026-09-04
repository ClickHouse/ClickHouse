// Root corpus index: points agents at the per-section corpora (see src/lib/corpus.ts).
import { config } from "virtual:nimbus/config";
import { PRIMARY, entriesFor, renderCorpusIndex, sectionsOf } from "../lib/corpus";

export const prerender = true;

export async function GET() {
  const sections = sectionsOf(await entriesFor(PRIMARY));
  return new Response(renderCorpusIndex(config.title, sections, ""), {
    headers: { "Content-Type": "text/plain; charset=utf-8" },
  });
}
