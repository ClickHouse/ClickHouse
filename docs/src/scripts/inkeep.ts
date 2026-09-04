/**
 * Nimbus-owned Inkeep bootstrap.
 *
 * Inkeep's `cxkit-mintlify` package provides the hosted search index.  Keep
 * the integration here instead of serving the former Mintlify customization:
 * the Nimbus search trigger has no Mintlify-specific IDs and page transitions
 * must not register a second keyboard shortcut on every navigation.
 */

declare global {
  interface Window {
    Inkeep?: {
      ModalSearchAndChat?: (settings: Record<string, unknown>) => void;
    };
  }
}

const SEARCH_TRIGGER = "[data-search-trigger]";
const SCRIPT_ID = "inkeep-cxkit-script";
const STYLE_ID = "inkeep-no-shift-style";
const SCRIPT_URL =
  "https://cdn.jsdelivr.net/npm/@inkeep/cxkit-mintlify@0.5/dist/index.js";

// These browser-facing keys are deliberately public.  The staging key selects
// the preview index; all other hosts use the production/local index as the
// legacy docs site did.
const STAGING_API_KEY = "d3e2792740610240ff7bcf2c2a78a33012812eb4f3e34d54";
const DEFAULT_API_KEY = "b25e5cf856ec9da60d250578b59dace8417359feeedcbc6b";
const PREVIEW_URL =
  /^https?:\/\/private-7c7dfe99\.mintlify\.(?:app|site)\/(?:docs(?:\/|(?=[?#]|$)))?/;

const topLevelTabs = ["Docs", "Changelogs", "Blogs", "Website", "GitHub"];
const docsSubareas = [
  "Get started",
  "Concepts",
  "Guides",
  "Reference",
  "Cloud",
  "ClickHouse Private",
  "Managed Postgres",
  "ClickStack",
  "Agentic Data Stack",
  "chDB",
  "Kubernetes Operator",
  "ClickPipes",
  "Connectors",
  "Language clients",
  "Ecosystem",
];
const docsSubareaRules = [
  ["get-started", "Get started"],
  ["concepts", "Concepts"],
  ["guides", "Guides"],
  ["reference", "Reference"],
  ["products/cloud", "Cloud"],
  ["products/bring-your-own-cloud", "Cloud"],
  ["products/clickhouse-private", "ClickHouse Private"],
  ["products/managed-postgres", "Managed Postgres"],
  ["products/agentic-data-stack", "Agentic Data Stack"],
  ["chdb", "chDB"],
  ["products/kubernetes-operator", "Kubernetes Operator"],
  ["clickstack", "ClickStack"],
  ["integrations/clickpipes", "ClickPipes"],
  ["integrations/connectors", "Connectors"],
  ["integrations/language-clients", "Language clients"],
  ["integrations", "Ecosystem"],
] as const;

function docsSubarea(url: string): string | null {
  const path = url
    .replace(/^https?:\/\/clickhouse\.com\/docs\//, "")
    .replace(/[?#].*$/, "");
  for (const [prefix, tab] of docsSubareaRules) {
    if (path === prefix || path.startsWith(`${prefix}/`)) return tab;
  }
  return null;
}

function twoRowTabCss(): string {
  const list = ".ikp-ai-search-results__tab-list";
  const tab = ".ikp-ai-search-results__tab";
  const notTopLevel = topLevelTabs
    .map((name) => `:not([id$="-trigger-${name}"])`)
    .join("");
  const subarea = `${tab}${notTopLevel}`;
  const topLevel = `${tab}:not(${subarea.slice(tab.length)})`;
  const docsActive = `${list}:has(${tab}[id$="-trigger-Docs"][data-state="active"])`;
  const subareaActive = `${list}:has(${subarea}[data-state="active"])`;
  return [
    `${list} { flex-wrap: wrap !important; overflow-x: visible !important; row-gap: 0.375rem; }`,
    `${tab} { font-size: 0.8125rem !important; min-height: 1.75rem !important; padding-inline: 0.625rem !important; border-radius: 12px !important; }`,
    `${topLevel} { order: 0; }`,
    `${subarea} { order: 2; display: none !important; box-shadow: inset 0 0 0 1px currentColor !important; }`,
    `${subarea}:not([data-state="active"]) { background: transparent !important; opacity: 0.7; }`,
    `${docsActive} ${subarea}, ${subareaActive} ${subarea} { display: inline-flex !important; }`,
    `${docsActive}::before, ${subareaActive}::before { content: ""; order: 1; flex: 0 0 100%; height: 1px; margin-block: 0.125rem; background: currentColor; opacity: 0.18; }`,
  ].join("");
}

function setUnavailable(): void {
  for (const trigger of document.querySelectorAll<HTMLElement>(
    SEARCH_TRIGGER,
  )) {
    trigger.setAttribute("aria-disabled", "true");
    trigger.title = "Search is temporarily unavailable.";
  }
}

function injectNoShiftStyle(): void {
  if (document.getElementById(STYLE_ID)) return;
  const style = document.createElement("style");
  style.id = STYLE_ID;
  style.textContent =
    "html body[data-scroll-locked] { padding-right: 0 !important; margin-right: 0 !important; }";
  document.head.appendChild(style);
}

function initialize(): void {
  const modal = window.Inkeep?.ModalSearchAndChat;
  if (!modal) {
    setUnavailable();
    return;
  }

  const initialQuery =
    new URLSearchParams(window.location.search).get("q") ?? "";
  const apiKey = /\.mintlify\.(?:app|site)$/.test(window.location.hostname)
    ? STAGING_API_KEY
    : DEFAULT_API_KEY;

  modal({
    defaultView: "search",
    modalSettings: {
      triggerSelector: SEARCH_TRIGGER,
      isOpen: Boolean(initialQuery),
    },
    baseSettings: {
      apiKey,
      primaryBrandColor: "#fdff75",
      organizationDisplayName: "ClickHouse",
      transformSource: (source: Record<string, unknown>) => {
        let url = typeof source.url === "string" ? source.url : "";
        const isPreview = PREVIEW_URL.test(url);
        if (isPreview)
          url = url.replace(PREVIEW_URL, "https://clickhouse.com/docs/");

        const tabs: string[] = [];
        if (isPreview || /clickhouse\.com\/docs(\/|$)/.test(url)) {
          if (/\/resources\/changelogs(\/|$)/.test(url))
            tabs.push("Changelogs");
          else {
            tabs.push("Docs");
            const subarea = docsSubarea(url);
            if (subarea) tabs.push(subarea);
          }
        } else if (url.includes("github.com") && /\/issues(\/|$)/.test(url)) {
          tabs.push("GitHub");
        } else if (/\/blog(\/|$)/.test(url)) {
          tabs.push("Blogs");
        } else if (url.includes("clickhouse.com")) {
          tabs.push("Website");
        }

        return { ...source, tabs, url };
      },
      colorMode: {
        sync: {
          target: document.documentElement,
          attributes: ["class"],
          isDarkMode: (attributes: Record<string, string> | undefined) =>
            attributes?.class?.includes("dark") ?? false,
        },
      },
      theme: {
        styles: [
          {
            key: "hide-inkeep-ai-chat",
            type: "style",
            value:
              ".ikp-view_toggle, .ikp-ai-ask-ai-trigger { display: none !important; }",
          },
          {
            key: "dark-search-overlay",
            type: "style",
            value:
              ".dark\\:bg-overlay-dark { background-color: rgba(0, 0, 0, 0.75) !important; }",
          },
          { key: "two-row-docs-tabs", type: "style", value: twoRowTabCss() },
        ],
      },
    },
    searchSettings: {
      placeholder: "Search ClickHouse docs...",
      defaultQuery: initialQuery,
      debounceTimeMs: 300,
      maxResults: 20,
      shouldShowContentSnippets: true,
      contentSnippetLength: 200,
      shouldHighlightMatches: true,
      tabs: [...topLevelTabs, ...docsSubareas],
    },
  });
}

function boot(): void {
  injectNoShiftStyle();
  if (document.getElementById(SCRIPT_ID)) return;

  const script = document.createElement("script");
  script.id = SCRIPT_ID;
  script.src = SCRIPT_URL;
  script.onload = initialize;
  script.onerror = setUnavailable;
  document.head.appendChild(script);
}

if (document.readyState === "loading")
  document.addEventListener("DOMContentLoaded", boot, { once: true });
else boot();
