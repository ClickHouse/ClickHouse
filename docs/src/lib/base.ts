/**
 * Astro `base` helpers. Nimbus 0.11 builds sidebar/breadcrumb/section hrefs
 * and llms/markdown URLs without the base (cloudflare/nimbus#105, #78), so the
 * owned routes and layouts apply it here.
 */
const raw = import.meta.env.BASE_URL ?? "/";
/** Base without trailing slash, e.g. "/docs" ("" when served at root). */
export const BASE = raw === "/" ? "" : raw.replace(/\/+$/, "");

export function withBase(path: string): string {
  if (!BASE) return path;
  if (!path.startsWith("/") || path.startsWith("//")) return path;
  if (path === BASE || path.startsWith(BASE + "/") || path.startsWith(BASE + "#") || path.startsWith(BASE + "?")) return path;
  return BASE + path;
}

/** Turn a browser pathname (with base) into a Nimbus route path (without). */
export function stripBase(pathname: string): string {
  if (BASE && (pathname === BASE || pathname.startsWith(BASE + "/"))) {
    const rest = pathname.slice(BASE.length);
    return rest === "" ? "/" : rest;
  }
  return pathname;
}

type AnyItem = { type?: string; href?: string; indexHref?: string; children?: AnyItem[] };

/** Prefix the base onto every in-site href of a Nimbus sidebar tree. */
export function prefixSidebarTree<T extends AnyItem>(items: T[]): T[] {
  return items.map((item) => {
    const next = { ...item } as T;
    if (typeof next.href === "string") next.href = withBase(next.href);
    if (typeof next.indexHref === "string") next.indexHref = withBase(next.indexHref);
    if (Array.isArray(next.children)) next.children = prefixSidebarTree(next.children);
    return next;
  });
}
