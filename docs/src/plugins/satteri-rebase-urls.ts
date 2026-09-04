import type { HastPluginDefinition } from "satteri";

/**
 * Prefixes Astro's `base` onto site-root-relative URLs in the rendered
 * content. The Mintlify content is written against `clickhouse.com/docs`
 * mounted at `/`, i.e. every internal link is `/reference/...`, so under
 * `base: "/docs"` they must become `/docs/reference/...`. Astro only rebases
 * its own asset URLs, never content links.
 *
 * Covers HTML elements (`a`, `img`, `source`, `video`, `iframe`) and the URL
 * props of MDX JSX components (`href`, `src`, `img`, `to`, `link`, `url`, …).
 */
export interface RebaseUrlsOptions {
  base: string;
  /** JSX component names whose string props are rebased. */
  components?: string[];
  /** Prop names treated as URLs on those components. */
  props?: string[];
}

// Lowercase HTML written as JSX inside MDX (`<img src="/images/...">`) arrives
// as an MDX JSX node, not a hast element, so the tags are listed here too.
const HTML_AS_JSX = ["a", "img", "source", "video", "iframe", "link"];
const DEFAULT_COMPONENTS = [
  "Image", "Card", "CUICard", "TrackedLink", "GalaxyTrackedLink", "PrimaryButton",
  "SecondaryButton", "Video", "Install", "LinkCard", "Frame", "Embed", "Update",
  "QuickstartPill", "HeroCard", "McpLink", "AskAILink", "IntegrationsBanner", "CsCard",
];
const DEFAULT_PROPS = ["href", "src", "img", "to", "link", "url", "image", "thumbnail", "poster"];

export function rebaseUrls(options: RebaseUrlsOptions): HastPluginDefinition {
  const base = options.base.replace(/\/+$/, "");
  const props = new Set(options.props ?? DEFAULT_PROPS);

  const rebase = (value: unknown): unknown => {
    if (typeof value !== "string") return value;
    // Root-relative, not protocol-relative, not already under the base.
    if (!value.startsWith("/") || value.startsWith("//")) return value;
    // Index pages are canonical at `/folder` (see pathId); `/folder/index` is a redirect.
    value = value.replace(/^(\/(?:[^#?]*\/)?)index(?=$|[#?])/, (_m, dir: string) => (dir === "/" ? "/" : dir.replace(/\/$/, "")));
    if (value === base || value.startsWith(base + "/") || value.startsWith(base + "#") || value.startsWith(base + "?")) return value;
    return base + value;
  };

  return {
    name: "clickhouse:rebase-urls",
    element: {
      filter: ["a", "img", "source", "video", "iframe", "link"],
      visit(node, ctx) {
        for (const key of ["href", "src", "poster"]) {
          const v = node.properties?.[key];
          const r = rebase(v);
          if (r !== v) ctx.setProperty(node, key, r);
        }
      },
    },
    mdxJsxFlowElement: {
      filter: [...HTML_AS_JSX, ...(options.components ?? DEFAULT_COMPONENTS)],
      visit(node) {
        return rebaseJsx(node, props, rebase);
      },
    },
    mdxJsxTextElement: {
      filter: [...HTML_AS_JSX, ...(options.components ?? DEFAULT_COMPONENTS)],
      visit(node) {
        return rebaseJsx(node, props, rebase);
      },
    },
  };
}

function rebaseJsx<N extends { attributes?: unknown[] }>(node: Readonly<N>, props: Set<string>, rebase: (v: unknown) => unknown): N | void {
  const attrs = (node.attributes ?? []) as Array<{ type: string; name?: string; value?: unknown }>;
  let changed = false;
  const next = attrs.map((a) => {
    if (a.type !== "mdxJsxAttribute" || !a.name || !props.has(a.name) || typeof a.value !== "string") return a;
    const r = rebase(a.value);
    if (r === a.value) return a;
    changed = true;
    return { ...a, value: r };
  });
  if (!changed) return;
  return { ...(node as N), attributes: next };
}
