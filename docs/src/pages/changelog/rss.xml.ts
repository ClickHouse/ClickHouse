import { getCollection } from "astro:content";
import { changelogPath, entriesForProduct } from "../../lib/changelog";

function xml(value: string): string {
  return value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;");
}

export async function GET() {
  const entries = entriesForProduct(await getCollection("changelog"));
  const items = entries.map((entry) => {
    const url = `https://clickhouse.com/docs${changelogPath(entry)}/`;
    return `<item><title>${xml(entry.data.title)}</title><link>${url}</link><guid>${url}</guid><pubDate>${new Date(`${entry.data.date}T00:00:00Z`).toUTCString()}</pubDate><description>${xml(entry.data.description)}</description></item>`;
  }).join("");
  return new Response(`<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>ClickHouse changelog</title><link>https://clickhouse.com/docs/changelog/</link><description>Release notes and product updates for ClickHouse.</description>${items}</channel></rss>`, { headers: { "Content-Type": "application/rss+xml; charset=utf-8" } });
}
