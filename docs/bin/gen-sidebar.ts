// Generates Nimbus `sidebar.items` from the Mintlify navigation
// (docs.json -> $ref navigation.json files) so the POC keeps today's exact
// order, grouping and labels. Also writes reports/nav-vs-disk.md.
//
//   node bin/gen-sidebar.ts            # English -> src/generated/sidebar.items.json
//   node bin/gen-sidebar.ts --locale es # -> src/generated/sidebar.items.es.json
import fs from "node:fs";
import path from "node:path";
import { parse as parseYaml } from "yaml";
import { execFileSync } from "node:child_process";
import { readScope } from "../src/lib/scope.ts";
import { apiMethodVariant, getApiOperationByPointer, getApiOperations, type HttpMethod } from "../src/lib/openapi.ts";

type Json = string | number | boolean | null | Json[] | { [k: string]: Json };
type Obj = { [k: string]: Json };

const root = process.cwd();
const ALL_LOCALES = ["ar", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh"];
// `--locale es` generates one locale; without it, English plus every locale in
// DOCS_LOCALES ("all" or a comma list), which is what `prebuild` relies on.
const localeArg = process.argv.indexOf("--locale");
const scope = readScope(root);
if (localeArg < 0 && !process.env.__GEN_SIDEBAR_CHILD) {
  for (const l of scope.locales) {
    execFileSync(process.execPath, [process.argv[1], "--locale", l], { stdio: "inherit", env: { ...process.env, __GEN_SIDEBAR_CHILD: "1" } });
  }
}
const locale = localeArg >= 0 ? process.argv[localeArg + 1] : null;
const contentRoot = locale ? path.join(root, locale) : root;
const urlPrefix = locale ? `/${locale}` : "";
const outDir = path.join(root, "src/generated");
fs.mkdirSync(outDir, { recursive: true });

// ---------------------------------------------------------------- loading
function readJson(p: string): Json {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
function resolveRefs(node: Json, baseDir: string): Json {
  if (Array.isArray(node)) return node.map((n) => resolveRefs(n, baseDir));
  if (node && typeof node === "object") {
    const o = node as Obj;
    if (typeof o.$ref === "string") {
      // `{ "$ref": "./x.json" }` is replaced by the file; sibling keys (e.g.
      // `{ "language": "es", "$ref": "./es/docs.json" }`) are kept on top.
      const p = path.normalize(path.join(baseDir, o.$ref));
      const target = resolveRefs(readJson(p), path.dirname(p));
      const rest = Object.fromEntries(Object.entries(o).filter(([k]) => k !== "$ref").map(([k, v]) => [k, resolveRefs(v, baseDir)]));
      return target && typeof target === "object" && !Array.isArray(target) ? { ...(target as Obj), ...rest } : target;
    }
    return Object.fromEntries(Object.entries(o).map(([k, v]) => [k, resolveRefs(v, baseDir)]));
  }
  return node;
}

// Locale navigation lives in `<locale>/docs.json` as a `{ language, tabs }`
// fragment pulled into the root file's `navigation.languages` via `$ref`, so
// every locale is resolved from the root docs.json.
const docsJsonPath = path.join(root, "docs.json");
const docsJson = resolveRefs(readJson(docsJsonPath), path.dirname(docsJsonPath)) as Obj;
const languages = ((docsJson.navigation as Obj).languages ?? []) as Obj[];
const lang = languages.find((l) => l.language === (locale ?? "en"));
if (!lang) throw new Error(`gen-sidebar: no navigation for language "${locale ?? "en"}" in docs.json`);
const tabs = (lang.tabs ?? []) as Obj[];

// ---------------------------------------------------------------- labels
type NavBadge = { text: string; variant: "success" | "info" | "warning" | "danger" | "note" };
const labelCache = new Map<string, { label: string; exists: boolean; badge?: NavBadge }>();
function pageInfo(page: string): { label: string; exists: boolean; badge?: NavBadge } {
  const cached = labelCache.get(page);
  if (cached) return cached;
  // Locale nav references are `es/...`; strip the locale so files resolve.
  const rel = locale && page.startsWith(locale + "/") ? page.slice(locale.length + 1) : page;
  const file = [".mdx", ".md"].map((e) => path.join(contentRoot, rel + e)).find((f) => fs.existsSync(f));
  let label = rel.split("/").pop() ?? rel;
  let badge: NavBadge | undefined;
  if (file) {
    const src = fs.readFileSync(file, "utf8");
    const m = src.match(/^---\r?\n([\s\S]*?)\r?\n---/);
    if (m) {
      try {
        const fm = (parseYaml(m[1]) ?? {}) as Record<string, unknown>;
        const st = fm.sidebarTitle ?? fm.title;
        if (typeof st === "string" && st.trim()) label = st.trim();
        if (typeof fm.openapi === "string") {
          const method = fm.openapi.match(/\b(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\s+\//i)?.[1]?.toLowerCase() as HttpMethod | undefined;
          if (method) badge = { text: method.toUpperCase(), variant: apiMethodVariant(method) };
        }
      } catch {
        // fall back to the path segment
      }
    }
  }
  const info = { label, exists: Boolean(file), badge };
  labelCache.set(page, info);
  return info;
}
// Preview builds omit the reference section (DOCS_REFERENCE=off); its links
// then point at production so the rail stays complete.
const referenceOff = !scope.reference;
const PRODUCTION_ORIGIN = "https://clickhouse.com/docs";

function pageLink(page: string): string {
  const rel = locale && page.startsWith(locale + "/") ? page.slice(locale.length + 1) : page;
  // The homepage is the native src/pages/index.astro route, not a collection page.
  if (rel === "index") return `${urlPrefix}/`;
  // Index pages are canonical at /folder (see pathId in src/content.config.ts).
  const link = `${urlPrefix}/${rel.replace(/\/index$/, "")}`;
  if (referenceOff && rel.startsWith("reference/")) return `${PRODUCTION_ORIGIN}${link}`;
  return link;
}

// ---------------------------------------------------------------- remotes
const remotesFile = path.join(root, "remotes.json");
const remotes: Array<{ name: string; repo: string; mount: string; sourceRef: string }> = fs.existsSync(remotesFile)
  ? (readJson(remotesFile) as { remotes: Array<{ name: string; repo: string; mount: string }> }).remotes.map((r) => ({
      ...r,
      // Mintlify's sourceRef names the GitHub repository (sometimes under an older name).
      sourceRef: r.repo,
    }))
  : [];
// The Mintlify config still says `ClickHouse/airgap-docs`; the repository is `airgapped-docs`.
for (const r of remotes) if (r.repo === "ClickHouse/airgapped-docs") r.sourceRef = "ClickHouse/airgap-docs";

/** Prefix every page path in a remote navigation tree with its mount directory. */
function prefixPages(nodes: Json[], mount: string): Json[] {
  return nodes.map((n) => {
    if (typeof n === "string") return `${mount}/${n}`;
    if (n && typeof n === "object" && !Array.isArray(n)) {
      const o = { ...(n as Obj) };
      if (Array.isArray(o.pages)) o.pages = prefixPages(o.pages as Json[], mount);
      return o;
    }
    return n;
  });
}

// ---------------------------------------------------------------- reports
const missing: string[] = [];
const skipped: string[] = [];
const orderDrift: string[] = [];
const seenPages = new Set<string>();

function noteOrder(groupPath: string, pages: string[]) {
  // Compare the authored order with the alphabetical disk order of the same set.
  const sorted = [...pages].sort((a, b) => a.localeCompare(b));
  if (pages.length > 1 && pages.some((p, i) => p !== sorted[i])) {
    orderDrift.push(`${groupPath}: ${pages.length} pages, authored order differs from alphabetical`);
  }
}

// ---------------------------------------------------------------- conversion
type NimbusItem =
  | { label: string; link: string; badge?: NavBadge }
  | { label: string; items: NimbusItem[]; collapsed?: boolean; segment?: string; landing?: string; icon?: string };

function convertPages(pages: Json[], groupPath: string): NimbusItem[] {
  const out: NimbusItem[] = [];
  const stringPages: string[] = [];
  for (const p of pages) {
    if (typeof p === "string") {
      stringPages.push(p);
      if (/^(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\s+\//i.test(p)) {
        const operation = getApiOperationByPointer("cloud", p);
        seenPages.add(p);
        out.push({
          label: operation.title,
          link: `/${operation.route}`,
          badge: { text: operation.method.toUpperCase(), variant: apiMethodVariant(operation.method) },
        });
        continue;
      }
      const info = pageInfo(p);
      if (!info.exists) missing.push(`${groupPath}: ${p}`);
      seenPages.add(p);
      out.push({ label: info.label, link: pageLink(p), badge: info.badge });
      continue;
    }
    if (p && typeof p === "object") {
      const o = p as Obj;
      if (typeof o.__apiLabel === "string" && typeof o.__apiLink === "string" && typeof o.__apiMethod === "string") {
        const method = o.__apiMethod as HttpMethod;
        out.push({ label: o.__apiLabel, link: o.__apiLink, badge: { text: method.toUpperCase(), variant: apiMethodVariant(method) } });
        continue;
      }
      if (typeof o.group === "string") {
        out.push(convertGroup(o, groupPath));
        continue;
      }
      if (Array.isArray(o.pages)) {
        out.push(...convertPages(o.pages as Json[], groupPath));
        continue;
      }
      skipped.push(`${groupPath}: ${JSON.stringify(o).slice(0, 120)}`);
    }
  }
  noteOrder(groupPath, stringPages);
  return out;
}

function convertGroup(g: Obj, parentPath: string): NimbusItem {
  const label = String(g.group);
  const groupPath = `${parentPath} > ${label}`;
  const openapi = g.openapi && typeof g.openapi === "object" && !Array.isArray(g.openapi) ? g.openapi as Obj : undefined;
  let sourcePages: Json[] = (g.pages as Json[]) ?? [];
  if (openapi && sourcePages.length === 0) {
    const directory = String(openapi.directory ?? "");
    const collection = directory.startsWith("clickstack/") ? "clickstack" : "cloud";
    const byTag = new Map<string, ReturnType<typeof getApiOperations>>();
    for (const operation of getApiOperations(collection)) {
      const operations = byTag.get(operation.tag) ?? [];
      operations.push(operation);
      byTag.set(operation.tag, operations);
    }
    sourcePages = [...byTag.entries()].map(([tag, operations]) => ({
      group: tag,
      pages: operations.map((operation) => ({
        __apiLabel: operation.title,
        __apiLink: `/${operation.route}`,
        __apiMethod: operation.method,
      })),
    }));
  }
  if (g.sourceRef !== undefined) {
    // Remote repository docs (remotes.json). When the fetcher has placed the
    // remote's own docs.json in its mount directory, its groups are expanded
    // here with page paths prefixed by the mount; otherwise the group is empty.
    const remote = remotes.find((r) => r.sourceRef === String(g.sourceRef));
    const remoteDocsJson = remote ? path.join(contentRoot, remote.mount, "docs.json") : null;
    if (remote && remoteDocsJson && fs.existsSync(remoteDocsJson)) {
      const rd = readJson(remoteDocsJson) as Obj;
      const groups = (((rd.navigation as Obj)?.groups ?? []) as Json[]);
      sourcePages = prefixPages(groups, remote.mount);
    } else {
      skipped.push(`${groupPath}: sourceRef ${String(g.sourceRef)} not fetched (run bin/fetch-remotes.ts)`);
    }
  }
  const items = convertPages(sourcePages, groupPath);
  // Preserve Mintlify's authored initial disclosure state. Collapsed groups
  // still load lazily, while expanded groups reproduce the live rail.
  const group: NimbusItem = {
    label,
    items,
    collapsed: typeof g.expanded === "boolean" ? !g.expanded : true,
    icon: typeof g.icon === "string" ? g.icon : undefined,
  };
  if (typeof g.root === "string") {
    (group as { segment?: string; landing?: string }).segment = pageLink(g.root);
    // The root's own page, when it exists, is where the label links.
    const rootPage = pageInfo(g.root + "/index").exists ? g.root + "/index" : g.root;
    if (pageInfo(rootPage).exists) (group as { landing?: string }).landing = pageLink(rootPage);
  }
  return group;
}

function convertMenu(menu: Obj[], tabPath: string): NimbusItem[] {
  // `href: "#"` entries are section headers: they own the items that follow.
  const out: NimbusItem[] = [];
  let current: { label: string; items: NimbusItem[]; collapsed?: boolean } | null = null;
  const push = (item: NimbusItem) => (current ? current.items.push(item) : out.push(item));
  for (const m of menu) {
    const label = String(m.item ?? m.group ?? "");
    if (m.href === "#") {
      current = { label, items: [], collapsed: false };
      out.push(current);
      continue;
    }
    if (typeof m.href === "string") {
      push({ label, link: m.href });
      continue;
    }
    if (Array.isArray(m.dropdowns)) {
      const dd = (m.dropdowns as Obj[]).map((d) => ({
        label: String(d.dropdown),
        items: convertPages((d.pages as Json[]) ?? [], `${tabPath} > ${label} > ${String(d.dropdown)}`),
        collapsed: true,
      }));
      push({ label, items: dd, collapsed: true });
      continue;
    }
    if (Array.isArray(m.pages)) {
      push({ label, items: convertPages(m.pages as Json[], `${tabPath} > ${label}`), collapsed: true });
      continue;
    }
    if (Array.isArray(m.groups)) {
      push({ label, items: (m.groups as Obj[]).map((g) => convertGroup(g, `${tabPath} > ${label}`)), collapsed: true });
      continue;
    }
    skipped.push(`${tabPath}: menu item ${JSON.stringify(m).slice(0, 120)}`);
  }
  return out;
}

const items: NimbusItem[] = tabs.map((t) => {
  const label = String(t.tab);
  if (Array.isArray(t.menu)) return { label, items: convertMenu(t.menu as Obj[], label), collapsed: false };
  if (Array.isArray(t.pages)) return { label, items: convertPages(t.pages as Json[], label), collapsed: false };
  if (Array.isArray(t.groups)) return { label, items: (t.groups as Obj[]).map((g) => convertGroup(g, label)), collapsed: false };
  skipped.push(`tab ${label}: unsupported shape`);
  return { label, items: [], collapsed: false };
});

const outFile = path.join(outDir, locale ? `sidebar.items.${locale}.json` : "sidebar.items.json");
fs.writeFileSync(outFile, JSON.stringify(items, null, 2) + "\n");

// ---------------------------------------------------------------- nav vs disk
const SECTIONS = ["get-started", "concepts", "guides", "reference", "products", "clickstack", "integrations", "resources", "chdb"];
const onDisk: string[] = [];
(function walk(dir: string) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    if (e.name.startsWith("_") || e.name === "node_modules") continue;
    const p = path.join(dir, e.name);
    if (e.isDirectory()) walk(p);
    else if (/\.mdx?$/.test(e.name) && !/README\.mdx?$/.test(e.name)) onDisk.push(path.relative(contentRoot, p).replace(/\.mdx?$/, ""));
  }
})(contentRoot);
const inSections = onDisk.filter((p) => SECTIONS.some((s) => p.startsWith(s + "/")));
const seenRel = new Set([...seenPages].map((p) => (locale && p.startsWith(locale + "/") ? p.slice(locale.length + 1) : p)));
const orphans = inSections.filter((p) => !seenRel.has(p));

const reportDir = path.join(root, "reports");
fs.mkdirSync(reportDir, { recursive: true });
const report = [
  `# Navigation vs disk (${locale ?? "en"}, generated ${new Date().toISOString().slice(0, 10)})`,
  "",
  `- Tabs: ${items.length}; pages referenced by navigation: ${seenPages.size}; pages on disk in content sections: ${inSections.length}.`,
  `- Navigation entries without a file: ${missing.length}.`,
  `- Files not referenced by navigation (orphans, still built and reachable by URL): ${orphans.length}.`,
  `- Groups whose authored order differs from alphabetical disk order: ${orderDrift.length}.`,
  `- Navigation constructs skipped by the generator: ${skipped.length}.`,
  "",
  "## Navigation entries without a file",
  ...missing.map((m) => `- ${m}`),
  "",
  "## Orphans (on disk, not in navigation)",
  ...orphans.map((o) => `- ${o}`),
  "",
  "## Groups with authored order != alphabetical",
  ...orderDrift.map((o) => `- ${o}`),
  "",
  "## Skipped constructs",
  ...skipped.map((s) => `- ${s}`),
  "",
].join("\n");
fs.writeFileSync(path.join(reportDir, locale ? `nav-vs-disk.${locale}.md` : "nav-vs-disk.md"), report);

console.log(
  `gen-sidebar: ${items.length} tabs, ${seenPages.size} pages -> ${path.relative(root, outFile)}; ` +
    `${missing.length} missing, ${orphans.length} orphans, ${orderDrift.length} groups reordered, ${skipped.length} skipped`,
);
