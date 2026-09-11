---
description: 'Documentation for the Web SQL UI (Play), the built-in in-browser query interface served at `/play`'
sidebar_label: 'Web SQL UI'
sidebar_position: 21
slug: /interfaces/web-sql
title: 'Web SQL UI (Play)'
doc_type: 'reference'
---

The Web SQL UI (Play) is the built-in, in-browser query interface for ClickHouse. It is served from any ClickHouse HTTP port at the `/play` path (for example, `http://localhost:8123/play`). It lets you write and run queries, view results as a table or a chart, and share a query by copying its URL.

The whole interface is contained in `programs/server/play.html`, a single self-contained page served directly from the ClickHouse binary, with no frameworks or build step. The one exception is chart rendering: the `uPlot` charting library is lazy-loaded from a third-party CDN the first time a result is displayed as a chart, so charts are unavailable in offline or egress-restricted deployments.

## Query tabs {#query-tabs}

Tabs let you keep multiple queries side by side instead of juggling them in a single editor or relying on browser history.

Each tab has its own query text, title, query parameters, and last result. The connection settings (URL, user, password) stay global and are shared by all tabs.

### When the tab bar appears {#when-the-tab-bar-appears}

The tab bar appears once a query has been run, or once there is more than one tab. A single, result-less tab looks exactly as the page did before tabs existed, so there is no visible tab bar until you need it.

The active tab merges visually into the page: its background tracks the per-query hash color (the same color the page background already uses), with a gradient that is more saturated at the top in the light theme and brighter at the top in the dark theme. Inactive tabs are tinted by the hash of their own query text, so different tabs are automatically distinguished by color.

### Creating, closing, and renaming tabs {#creating-closing-and-renaming-tabs}

- Create a new tab with the `[+]` button to the right of the tabs.
- Close a tab with the `x` icon on the tab.
- New tabs get default names `Query A`, `Query B`, and so on.
- Click the title of the active tab to edit it inline; the editing field grows to fit the text.

### Switching tabs {#switching-tabs}

- Click an inactive tab to switch to it.
- Scroll the mouse wheel over the tab panel to switch tabs: scrolling up moves to the tab on the left, scrolling down to the tab on the right (whichever exist). Both vertical and horizontal wheel scrolling work.

The tab bar is horizontally sticky — it stays at the left during horizontal scroll of the page, like the ClickHouse logo at the bottom — and it scrolls away vertically with the rest of the page.

### Persistence and browser history {#persistence-and-browser-history}

The workspace — the tabs, their titles, the active tab, their order, and small result snapshots — is persisted in IndexedDB and restored on reload. Persistence is best-effort: if IndexedDB is unavailable, the workspace degrades to in-memory state for the current session.

Tabs also integrate with the browser's History API and the URL:

- The history state carries the active tab, so the browser's back and forward buttons switch tabs.
- The URL gains a `tab=<name>` parameter. On load, the URL's query and `tab` parameter are reconciled with the saved tabs: an existing tab with that name is reused (and its query is substituted), or a new tab is created when the name is not found or was unnamed. This makes it possible to open a URL with a new query while keeping your own saved tabs.

### Limitations {#limitations}

Switching tabs while a query is running discards the running state of that query.

Only small results are snapshotted for restore. A large result (above the snapshot size limit) or an image result is not persisted: after a tab switch or reload the tab keeps its query but not the rendered result, and re-running the query reproduces it. This applies both to single-query results and to the combined output of a "Run all" (multi-query) run.
