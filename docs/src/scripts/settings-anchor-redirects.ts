/**
 * Redirect legacy setting hashes to the category pages that now contain them.
 *
 * The generated Mintlify tables remain the source of truth during the POC.
 * Each table is code-split and read only for its matching setting index page;
 * the historical perpetual animation-frame observer is deliberately not
 * carried forward.
 */

type RouteTable = Record<string, string>;

const loaders: Record<string, () => Promise<{ default: string }>> = {
  "/reference/settings/session-settings": () =>
    import("../../_site/customizations/settings-legacy-routes/session-settings.js?raw"),
  "/reference/settings/server-settings/settings": () =>
    import("../../_site/customizations/settings-legacy-routes/server-settings.js?raw"),
  "/reference/settings/merge-tree-settings": () =>
    import("../../_site/customizations/settings-legacy-routes/mergetree-settings.js?raw"),
  "/reference/settings/formats": () =>
    import("../../_site/customizations/settings-legacy-routes/format-settings.js?raw"),
};

function parseTable(source: string): RouteTable {
  const match = source.match(/= (\{.*\});\s*$/s);
  if (!match) throw new Error("Invalid legacy settings redirect table.");
  return JSON.parse(match[1]) as RouteTable;
}

function canonicalAnchor(table: RouteTable, value: string): string | undefined {
  for (const candidate of [value, value.replace(/[?,;:!'"()[\]{}]/g, "")]) {
    if (table[candidate]) return candidate;
    const lowercase = candidate.toLowerCase();
    if (table[lowercase]) return lowercase;
  }
  return undefined;
}

async function redirect(): Promise<void> {
  const rawHash = window.location.hash.slice(1);
  if (!rawHash) return;

  let decodedHash: string;
  try {
    decodedHash = decodeURIComponent(rawHash);
  } catch {
    return;
  }

  for (const [root, load] of Object.entries(loaders)) {
    const markerIndex = window.location.pathname.indexOf(root);
    if (markerIndex < 0) continue;
    if (
      window.location.pathname
        .slice(markerIndex + root.length)
        .replace(/\/$/, "")
    )
      continue;

    const table = parseTable((await load()).default);
    const direct = canonicalAnchor(table, decodedHash);
    const base = direct ?? canonicalAnchor(table, decodedHash.split("-", 1)[0]);
    if (!base) return;

    const prefix = window.location.pathname.slice(0, markerIndex);
    window.location.replace(
      `${prefix}${table[base]}${window.location.search}#${direct ?? rawHash}`,
    );
    return;
  }
}

void redirect();
document.addEventListener("astro:page-load", () => void redirect());
