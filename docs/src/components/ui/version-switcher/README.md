# VersionSwitcher

Header dropdown + mobile sidebar control for switching docs versions.

Reads from the framework's data layer (`getVersions`,
`getVersionAlternates`, `getCurrentVersion`). Renders nothing when the
site is unversioned or has only one version.

## After `nimbus-docs add version-switcher`, wire two placements

The recipe installs the component files but does **not** edit your
layouts. Two manual wires:

### 1. Desktop header

Edit `src/components/Header.astro`. Accept `collection` and `entryId`
as props, then render:

```astro
---
import { VersionSwitcher } from "./ui/version-switcher";

interface Props {
  collection?: string;
  entryId?: string;
}

const { collection, entryId } = Astro.props;
---

{/* ... existing header content ... */}
<div class="flex items-center gap-2">
  <VersionSwitcher collection={collection} entryId={entryId} />
  {/* ... other header controls (search, theme toggle, etc.) ... */}
</div>
```

### 2. Mobile sidebar

Edit `src/layouts/DocsLayout.astro`. Inside the mobile sidebar drawer
(the `<dialog data-mobile-sidebar>` block), add the picker above the
sidebar nav:

```astro
<nav class="px-4 pb-8 pt-5">
  <VersionSwitcher
    collection={collection}
    entryId={entryId}
    variant="sidebar"
  />
  <SidebarFilter />
  <Sidebar items={sidebar} />
</nav>
```

The `variant="sidebar"` mode has no built-in breakpoint gating — it
inherits visibility from its container, so it works in a mobile drawer
(shown below `md`) and in a persistent desktop sidebar (like the API
reference's left column, shown above `md`) alike. The `header` variant
is the dropdown for a top bar. Pass a `class` to gate visibility if a
placement needs it.

### 3. Pass props through your layout chain

`DocsLayout.astro` should already accept `collection` and `entryId` as
props (set up by the `new-collection` recipe or the framework's default
route scaffolding) and forward them to `Header`:

```astro
<Header collection={collection} entryId={entryId} />
```

If your layout doesn't do this yet, see the framework spec for the
required props chain: `route → DocsLayout → BaseLayout → NimbusHead` +
`route → DocsLayout → Header → VersionSwitcher`.

## Props

| Prop | Type | Default | Notes |
|---|---|---|---|
| `collection` | `string` | `undefined` | `entry.collection` from your route. Drives the "stay on this page" logic for the current version. |
| `entryId` | `string` | `undefined` | `entry.id` from your route. Drives per-version `href` via the alternates table. |
| `variant` | `"header"` \| `"sidebar"` | `"header"` | Desktop popover or mobile sidebar section. |

Both placements share the same data; render both and let CSS pick.
