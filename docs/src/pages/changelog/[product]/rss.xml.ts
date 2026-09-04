import { getCollection } from "astro:content";
import { changelogPath, entriesForProduct, productTitle, type ChangelogEntry } from "../../../lib/changelog";

function xml(value: string): string {
  return value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;");
}

export async function getStaticPaths() {
  const entries = await getCollection("changelog");
  return ["cloud", "oss"].map((product) => ({ params: { product }, props: { entries: entriesForProduct(entries, product) } }));
}

export function GET({ params, props }: { params: { product: string }; props: { entries: ChangelogEntry[] } }) {
  const title = `${productTitle(params.product)} changelog`;
  const items = props.entries.map((entry) => {
    const url = `https://clickhouse.com/docs${changelogPath(entry)}/`;
    return `<item><title>${xml(entry.data.title)}</title><link>${url}</link><guid>${url}</guid><pubDate>${new Date(`${entry.data.date}T00:00:00Z`).toUTCString()}</pubDate><description>${xml(entry.data.description)}</description></item>`;
  }).join("");
  return new Response(`<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>${xml(title)}</title><link>https://clickhouse.com/docs/changelog/${params.product}/</link><description>Release notes for ${xml(productTitle(params.product))}.</description>${items}</channel></rss>`, { headers: { "Content-Type": "application/rss+xml; charset=utf-8" } });
}
