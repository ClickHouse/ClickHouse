---
description: 'Documentation for the embedded SQL Console UI served from the ClickHouse HTTP server at `/ui`'
sidebar_label: 'SQL Console'
sidebar_position: 23
slug: /interfaces/sql-console
title: 'SQL Console'
doc_type: 'reference'
---

The SQL Console is an embedded web UI for running queries and exploring data. It is served from any ClickHouse HTTP port at the `/ui` path, for example `http://localhost:8123/ui`.

:::note
The SQL Console is an experimental feature.
:::

## Overview {#overview}

The SQL Console is a single-page application (SPA) that is compiled into a static bundle and embedded directly into the ClickHouse binary, similar to how ClickStack is embedded at `/clickstack`. All assets are served from the ClickHouse binary itself — no third-party CDNs are loaded.

To open it, navigate to `/ui` on any ClickHouse HTTP port (for example, `http://localhost:8123/ui`).

## How it is served {#how-it-is-served}

The bundle is gzipped at build time and embedded with `#embed`, so every response from `/ui` carries a `Content-Encoding: gzip` header. Because the SQL Console is a single-page application that performs client-side routing, requests under `/ui` that do not map to an embedded asset and that do not look like a file (i.e. have no extension) fall back to serving `index.html`. Requests to paths that look like missing assets (for example `/ui/nonexistent.js`) return HTTP status `404 Not Found`.

The `/ui` path is matched exactly or when followed by a `/`, `?`, or `#` boundary, so sibling routes such as `/uix` are not hijacked by the SPA fallback.

## Relationship to the other web UIs {#relationship-to-other-web-uis}

ClickHouse ships several built-in web interfaces served from the HTTP port:

- [`/play`](/interfaces/http) — the lightweight Web SQL UI.
- `/dashboard` — the built-in dashboards.
- `/clickstack` — the embedded ClickStack UI.
- `/ui` — the SQL Console described here.

Links to all of these are available on the ClickHouse HTTP server landing page.

## Authentication {#authentication}

The SQL Console authenticates against the same `Session` and access-control checks as the HTTP protocol. Serving the static UI assets at `/ui` does not require authentication; queries issued by the UI are authenticated like any other HTTP request.
