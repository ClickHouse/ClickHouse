/**
 * Per-section /<section>/llms.txt — sub-index files that drill down
 * from the root `/llms.txt` into a named slice of the site's docs.
 *
 * A "section" is one of two things:
 *   1. A folder inside the primary `docs` collection with more than
 *      one page (e.g. `src/content/docs/<folder>/*` → `/<folder>/llms.txt`).
 *   2. A whole non-primary collection — `api`, `blog`, etc. — which
 *      becomes a single section mounted at `/<collection>/llms.txt`.
 *
 * Both cases produce the same shape at the same URL pattern, so
 * agents follow one rule: every link in `/llms.txt` that ends in
 * `.llms.txt` resolves here.
 *
 * `getIndexedTopLevel()` decides which sections exist and what they
 * contain; this route just renders one file per section it returns.
 */

import { getIndexedTopLevel, type IndexedEntry } from "@cloudflare/nimbus-docs";
import { config } from "virtual:nimbus/config";
import { withBase } from "../../lib/base";

export const prerender = true;

interface SectionProps {
  slug: string;
  label: string;
  members: IndexedEntry[];
}

export async function getStaticPaths() {
  const { groups } = await getIndexedTopLevel();
  return groups
    // Versioning: hidden versions don't get a per-section llms.txt
    // index. They're URL-reachable for direct navigation, but every
    // agent-discovery surface should treat them as if they don't exist.
    .filter((group) => !group.hidden)
    .map((group) => ({
      params: { section: group.slug },
      props: {
        slug: group.slug,
        label: group.label,
        members: group.members,
      } as SectionProps,
    }));
}

export async function GET({ props }: { props: SectionProps }) {
  const { label, members } = props;

  const lines = [`# ${label}`, "", "## Pages", ""];

  for (const item of members) {
    const description = item.description ? ` — ${item.description}` : "";
    lines.push(
      `- [${item.title}](${new URL(withBase(item.markdownUrl), config.site).href})${description}`,
    );
  }

  lines.push("");

  return new Response(lines.join("\n"), {
    headers: { "Content-Type": "text/plain; charset=utf-8" },
  });
}
