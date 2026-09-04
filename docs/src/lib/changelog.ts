import type { CollectionEntry } from "astro:content";

export type ChangelogEntry = CollectionEntry<"changelog">;

export function productSlug(product: string): string {
  return product === "ClickHouse Cloud" ? "cloud" : "oss";
}

export function entriesForProduct(entries: ChangelogEntry[], product?: string): ChangelogEntry[] {
  return entries
    .filter((entry) => !entry.data.hidden)
    .filter((entry) => !product || entry.data.products.some((value) => productSlug(value) === product))
    .sort((left, right) => right.data.date.localeCompare(left.data.date) || right.id.localeCompare(left.id));
}

export function productTitle(product: string): string {
  return product === "cloud" ? "ClickHouse Cloud" : "ClickHouse";
}

export function changelogPath(entry: ChangelogEntry): string {
  return `/changelog/${entry.id}`;
}
