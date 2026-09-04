/**
 * Lazy sidebar groups. Collapsed groups without the active page render as a
 * placeholder that fetches its children from a prebuilt HTML fragment
 * (`/nav/<key>/`) on first expand; without this every page carries the
 * whole section tree twice (desktop rail + mobile drawer), ~1.2 MB.
 *
 * Keys are label paths below the tab level, so the fragment route (built from
 * the generated config tree) and the page rail (Nimbus's rendered tree, whose
 * top level is already the active tab's children) agree.
 */
import type { SidebarItem } from "@cloudflare/nimbus-docs/types";
import { withBase } from "./base";

/** Groups with fewer children than this are rendered inline even when collapsed. */
export const LAZY_MIN_CHILDREN = 6;

export const TAB_LABELS = new Set(["Home", "Database", "Solutions", "Integrations", "Resources"]);

export type ConfigItem =
  | { label: string; link: string; badge?: import("@cloudflare/nimbus-docs/types").SidebarBadge }
  | { label: string; items: ConfigItem[]; collapsed?: boolean; segment?: string; landing?: string; icon?: string };

export function slugifyLabel(label: string): string {
  return label
    .normalize("NFKD")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "") || "group";
}

/** Deduplicate sibling keys deterministically (same order on both sides). */
function siblingKeys(labels: string[]): string[] {
  const seen = new Map<string, number>();
  return labels.map((l) => {
    const base = slugifyLabel(l);
    const n = seen.get(base) ?? 0;
    seen.set(base, n + 1);
    return n === 0 ? base : `${base}-${n + 1}`;
  });
}

export type LazyGroup = { key: string; label: string; items: ConfigItem[]; path: string[] };

/** Every group in the config tree with its fragment key. Tabs are skipped. */
export function collectGroups(items: ConfigItem[], path: string[] = [], out: LazyGroup[] = [], depth = 0): LazyGroup[] {
  const groups = items.filter((i): i is Extract<ConfigItem, { items: ConfigItem[] }> => "items" in i);
  const keys = siblingKeys(groups.map((g) => g.label));
  groups.forEach((g, idx) => {
    // The generator always emits the tabs at the top level (labels are
    // translated in locale trees, so detection is structural, not by label).
    const isTab = depth === 0;
    const nextPath = isTab ? path : [...path, keys[idx]];
    if (!isTab) out.push({ key: nextPath.join("/"), label: g.label, items: g.items, path: nextPath });
    collectGroups(g.items, nextPath, out, depth + 1);
  });
  return out;
}

/** Config items -> the rendered SidebarItem shape the UI components take. */
export function toRendered(items: ConfigItem[], path: string[]): SidebarItem[] {
  const groups = items.filter((i): i is Extract<ConfigItem, { items: ConfigItem[] }> => "items" in i);
  const keys = siblingKeys(groups.map((g) => g.label));
  let gi = 0;
  return items.map((item, order) => {
    if ("items" in item) {
      const key = [...path, keys[gi++]].join("/");
      const group = {
        type: "group" as const,
        label: item.label,
        order,
        collapsed: item.collapsed ?? true,
        children: toRendered(item.items, key.split("/")),
        indexHref: item.landing ? withBase(item.landing) : undefined,
        segment: item.segment,
        icon: item.icon,
      } as SidebarItem & { _lazyKey?: string };
      group._lazyKey = key;
      return group;
    }
    const href = /^(https?:)?\/\//.test(item.link) ? item.link : withBase(item.link);
    return /^(https?:)?\/\//.test(item.link)
      ? ({ type: "external", label: item.label, href, order, badge: item.badge } as SidebarItem)
      : ({ type: "link", label: item.label, href, order, badge: item.badge } as SidebarItem);
  });
}

/** Annotate Nimbus's rendered tree with the same keys (rail side). */
export function assignLazyKeys<T extends SidebarItem>(items: T[], path: string[] = []): T[] {
  const groups = items.filter((i) => i.type === "group") as Array<Extract<SidebarItem, { type: "group" }>>;
  const keys = siblingKeys(groups.map((g) => g.label));
  let gi = 0;
  return items.map((item) => {
    if (item.type !== "group") return item;
    const isTab = path.length === 0 && TAB_LABELS.has(item.label);
    const key = isTab ? [] : [...path, keys[gi]];
    gi++;
    const next = { ...item, children: assignLazyKeys(item.children, key) } as T & { _lazyKey?: string };
    if (!isTab) next._lazyKey = key.join("/");
    return next;
  });
}

/**
 * Rail built directly from the generated config tree, scoped to the active
 * top-navigation menu entry rather than the whole tab. For example, pages in
 * `Solutions > ClickHouse Cloud` receive the Cloud rail, never the Managed
 * Postgres, ClickStack, or other Solutions trees. `keyPrefix` namespaces the
 * lazy fragments (`/nav/<locale>/...`).
 */
export function buildRailFromConfig(items: ConfigItem[], currentPath: string, keyPrefix: string[] = []): SidebarItem[] {
  const norm = (p: string) => p.replace(/\/+$/, "") || "/";
  const target = norm(currentPath);
  const tabs = items.filter((i): i is Extract<ConfigItem, { items: ConfigItem[] }> => "items" in i);
  const contains = (nodes: ConfigItem[]): boolean =>
    nodes.some((n) => ("items" in n ? contains(n.items) : norm(withBase(n.link)) === target));
  const tab = tabs.find((t) => contains(t.items)) ?? tabs[0];
  if (!tab) return [];

  const groupsWithKeys = (nodes: ConfigItem[]) => {
    const groups = nodes.filter((i): i is Extract<ConfigItem, { items: ConfigItem[] }> => "items" in i);
    const keys = siblingKeys(groups.map((group) => group.label));
    return groups.map((group, index) => ({ group, key: keys[index] }));
  };

  // Database, Integrations, and Resources expose their direct groups in the
  // top menu. Solutions adds a presentational section level (ClickHouse Cloud
  // and Open source), so select its actual product entry one level deeper.
  const topMatch = groupsWithKeys(tab.items).find(({ group }) => contains(group.items));
  let railItems = tab.items;
  let railPath = keyPrefix;
  if (topMatch) {
    railItems = topMatch.group.items;
    railPath = [...keyPrefix, topMatch.key];
    if (tab.label === "Solutions") {
      const productMatch = groupsWithKeys(topMatch.group.items).find(({ group }) => contains(group.items));
      if (productMatch) {
        railItems = productMatch.group.items;
        railPath = [...railPath, productMatch.key];
      }
    }
  }

  const rendered = toRendered(railItems, railPath);
  const mark = (nodes: SidebarItem[]): boolean => {
    let any = false;
    for (const n of nodes) {
      if (n.type === "link") {
        if (norm(n.href) === target) { (n as { isCurrent?: boolean }).isCurrent = true; any = true; }
      } else if (n.type === "group") {
        const hit = mark(n.children);
        if (hit) { n.collapsed = false; any = true; }
      }
    }
    return any;
  };
  mark(rendered);
  return rendered;
}

/** Top-level sections (tabs) for the header, from the generated config tree. */
export function sectionsFromConfig(items: ConfigItem[], currentPath: string): Array<{ label: string; href: string; isActive: boolean }> {
  const norm = (p: string) => p.replace(/\/+$/, "") || "/";
  const target = norm(currentPath);
  const firstLink = (nodes: ConfigItem[]): string | undefined => {
    for (const n of nodes) {
      if ("items" in n) { const l = firstLink(n.items); if (l) return l; }
      else if (!/^(https?:)?\/\//.test(n.link)) return n.link;
    }
    return undefined;
  };
  const contains = (nodes: ConfigItem[]): boolean =>
    nodes.some((n) => ("items" in n ? contains(n.items) : norm(withBase(n.link)) === target));
  return items
    .filter((i): i is Extract<ConfigItem, { items: ConfigItem[] }> => "items" in i)
    .map((t) => ({ label: t.label, href: withBase(firstLink(t.items) ?? "/"), isActive: contains(t.items) }));
}
