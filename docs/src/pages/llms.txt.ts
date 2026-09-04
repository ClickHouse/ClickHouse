// Root /llms.txt — sectioned index for AI agents.
import { getIndexedTopLevel } from "@cloudflare/nimbus-docs";
import { config } from "virtual:nimbus/config";
import { withBase } from "../lib/base";

export const prerender = true;

export async function GET() {
  const { leaves, groups } = await getIndexedTopLevel();

  const lines = [
    `# ${config.title}`,
    "",
    config.description ?? "Documentation index for AI agents.",
    "",
    `Full corpus (all pages, one document): ${new URL(withBase("/llms-full.txt"), config.site).href}`,
    "",
    "## Pages",
    "",
  ];

  // Sort leaves + groups alphabetically into a single stable list.
  type Row = { key: string; line: string };
  const rows: Row[] = [];

  for (const leaf of leaves) {
    const description = leaf.description ? ` — ${leaf.description}` : "";
    rows.push({
      key: leaf.url,
      line: `- [${leaf.title}](${new URL(withBase(leaf.markdownUrl), config.site).href})${description}`,
    });
  }

  for (const group of groups) {
    // Older doc versions have their own /<v>/llms.txt; don't list them here.
    if (group.kind === "version") continue;
    rows.push({
      key: `/${group.slug}`,
      line: `- [${group.label}](${new URL(withBase(`/${group.slug}/llms.txt`), config.site).href})`,
    });
  }

  rows.sort((a, b) => a.key.localeCompare(b.key));
  for (const row of rows) lines.push(row.line);

  lines.push("");

  return new Response(lines.join("\n"), {
    headers: { "Content-Type": "text/plain; charset=utf-8" },
  });
}
