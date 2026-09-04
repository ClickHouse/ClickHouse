---
description: "Local Project Nimbus proof-of-concept validation for ClickHouse documentation"
sidebarTitle: "Nimbus POC"
slug: "/internal/nimbus-poc"
title: "Project Nimbus POC validation"
doc_type: "report"
---

# Project Nimbus POC validation {#project-nimbus-poc-validation}

## Outcome {#outcome}

The local POC validates that Nimbus can render the ClickHouse documentation corpus under
`/docs`, retain the authored navigation and anchors, serve all eight locale trees, and replace
the principal Mintlify chrome integrations. It is not ready for a production cutover yet: the
native OpenAPI capability assumed by the plan is absent in the pinned Nimbus package, and the
Workers Builds/R2 preview and reference-bundle gates need provisioned infrastructure.

## Local evidence {#local-evidence}

| Area | Result |
| --- | --- |
| English parity | 2,657 pages built in 1 min 35 s; explicit anchors 19,745/19,745; no base-path misses. |
| Locales | Full nine-tree build rendered 28,149 pages in 14 min 12 s and 54,406 postbuild files, including the full authored homepage in every locale. |
| Worker | Local Worker smoke verified HTML, Markdown negotiation, redirect, and locale-aware Markdown 404 behavior. `wrangler deploy --dry-run` packages successfully. |
| P4 chrome | Inkeep search, Kapa Ask AI, fragment/settings redirects, signup CTA, issue link, web terminal, Galaxy analytics transport, full authored homepage islands, and build-time SQL highlighting are implemented. |
| Changelog | 689 OSS releases and 18 Cloud updates materialize into 707 isolated entries; the index, product pages, entry pages, and three RSS feeds build successfully. |

The latest changelog-inclusive English build completed in 58.04 s, rendered 3,757 pages, and
produced 9,213 postbuild files. It had no errors. Known non-blocking advisories are Vite's
deprecated `esbuild` configuration, the >500 kB chunk advisory, and 144 Shiki unsupported
language fallbacks to plain text.

## Measured limits {#measured-limits}

The nine-locale output is below the 100,000 Workers static-asset limit. The largest local
English build observed 2.2 GB peak RSS; English plus Spanish used 4.9 GB. The full production
build therefore needs a first Workers Builds measurement against the 8 GB limit before it can
be accepted as meeting the 20-minute production gate.

URL parity is 95.96% (22,963/23,931 sitemap paths) across all locales. The explained misses are
the deferred Cloud and ClickStack API endpoints, private remote documentation, and one stale
sitemap URL. Existing generated Cloud API MDX still renders at its current paths; the deficit is
the planned spec-driven replacement, not a newly introduced 404.

## Outstanding gates {#outstanding-gates}

1. The pinned `@cloudflare/nimbus-docs` 0.11.0 package has no `api` configuration or OpenAPI
   renderer. Choose an upstream Nimbus release that supplies this feature, or approve an owned
   OpenAPI renderer before replacing the current generated API pages.
2. Provision the `clickhouse-docs` Workers Builds project, preview and production credentials,
   R2 bucket, Praktika secrets, and the website Worker's `DOCS` service binding. Then perform a
   fork-preview test, full production build measurement, third-party event observation, and a
   rollback rehearsal.
3. Implement and exercise the nightly reference bundle producer/consumer once its R2 bucket is
   available. The POC currently uses committed reference MDX, which is the planned phase-A
   behavior and avoids overlap with a future bundle.
4. Perform the P4 side-by-side visual review in a provisioned preview. Local interactive review
   was intentionally deferred while the site preview was not running.

## Recommendation {#recommendation}

Proceed with the infrastructure-provisioning and upstream/OpenAPI decision in parallel. The
content, route, locale, redirect, Worker, and chrome migration work is locally viable. Do not
enable the `docs_nimbus_cutover` flag until every outstanding gate above is resolved and the
live preview/rollback checks are recorded.
