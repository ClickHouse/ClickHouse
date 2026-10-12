---
description: 'Documentation for the built-in documentation search Web UI, served at the `/docs` path of the HTTP interface and backed by the `system.documentation` table'
sidebarTitle: 'Documentation Search'
slug: /interfaces/documentation-search
title: 'Documentation Search'
doc_type: 'reference'
---

The documentation search page is a small, self-contained Web UI for instant search over the embedded reference documentation. It is served from any ClickHouse HTTP port at the `/docs` path.

Navigate to `/docs` on any ClickHouse HTTP port (for example, `http://localhost:8123/docs`) to open it.

## What it does {#what-it-does}

The page queries the [`system.documentation`](/reference/system-tables/documentation) table over HTTP as you type, and renders the selected entity's Markdown. Because it reads `system.documentation`, it covers every entity that table exposes — functions, aggregate functions, table functions, table engines, database engines, data types, settings, formats, compression codecs, profile events, metrics, the system tables themselves, and more — and always matches the documentation embedded in the running server.

Type in the search box and the matches appear in a type-color-coded list; selecting a match renders its documentation. Rendering includes:

- a pencil link next to the entity's title that opens its source file on GitHub, taken from the `source` column of `system.documentation`;
- ClickHouse SQL syntax highlighting of code blocks, using the same embedded lexer (`Lexer.wasm`) as the [`/play`](/concepts/features/interfaces/http) UI;
- TeX math via [KaTeX](https://katex.org/) (for example, the formula on the `corr` page);
- `:::note`/`:::tip`/… admonitions, heading anchors with shareable links, and a hover "Copy" button on code blocks;
- relative links resolved to another documented entity in-app when it exists, otherwise to `https://clickhouse.com/docs`; "Related" and "Alias of" references become in-app links.

The current search term, open entity, and section are mirrored into the URL fragment, so a particular page or section can be linked to directly and is restored by browser back/forward navigation. A light/dark theme switcher (with autodetection) matches `/play`.

## Connecting {#connecting}

The header has `URL`, `user`, and `password` inputs, exactly like `/play`. When the page is served by ClickHouse, the `URL` defaults to the current origin; when the page is opened as a local file, it defaults to `http://localhost:8123/`, so the page can also be opened locally against a remote server. The cross-link name cache is rebuilt automatically when the connection changes.

## Assets {#assets}

All assets — including the Markdown renderer ([Marked](https://marked.js.org/)), the math renderer (KaTeX, with its fonts), and the SQL lexer — are served from the ClickHouse binary itself when the page is served over HTTP. No third-party CDNs are loaded in the ClickHouse HTTP origin, so the page is self-contained, works offline, and does not execute third-party network code alongside the credentials it handles.

## Security considerations {#security}

The page issues queries to the ClickHouse HTTP endpoint with the credentials entered in the header, so the same caveats that apply to the HTTP protocol apply here:

- Always serve `/docs` over HTTPS in untrusted environments to protect credentials.
- Restrict access at the network level (firewall, reverse proxy, or the `listen_host` configuration) the same way you restrict access to the HTTP protocol.

`system.documentation` contains only the static reference documentation embedded in the server, so the page does not expose any data from your tables.
