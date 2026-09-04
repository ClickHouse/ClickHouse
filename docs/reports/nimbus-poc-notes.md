# Project Nimbus: POC notes and measurements (2026-09-03)

Scope: `reference/functions/regular-functions/other-functions.mdx`,
`reference/settings/session-settings/**`, `guides/clickhouse/working-with-arrays.mdx`,
`resources/changelogs/cloud/2026.mdx`, then the whole `reference/` tree.

## Results

- The P0 slice completed on 2026-09-03: 263 pages in 17.57 s on a laptop
  (Astro 7.0.9, Nimbus 0.11.0, Sätteri 0.10.5). The slice includes the five
  planned paths, including the 1.4 MB OSS changelog page.
- Custom heading anchors: 1,564/1,564 explicit `{#id}` anchors were present
  in the output (`bin/measure/anchor-parity.ts`).
- Base path: all 140,407 checked site-relative URLs were under `/docs`
  (`bin/measure/base-check.ts`). Content URLs are rebased by
  `src/plugins/satteri-rebase-urls.ts`; Nimbus chrome is fixed in owned code
  (see below).
- Astro's content cache does not include `DOCS_INCLUDE` in its invalidation
  key. Clear `docs/.astro` before a differently scoped P0 build; otherwise a
  previous full collection can be rendered despite the requested scope.
- A second, Spanish collection spike completed on 2026-09-03: 21 Spanish
  pages plus their English counterparts built in 13.69 s. The collection-aware
  route emits localized `/docs/es/...` sidebar and breadcrumb links, confirming
  that `getSidebar`, `getPrevNext`, `getBreadcrumbs`, and the Markdown routes
  accept the collection parameter.
- In a scoped collection build, Nimbus's filesystem sidebar can still expose
  locale files excluded by `DOCS_INCLUDE`. This is harmless for a full build
  but would produce dead links in a preview. P1's generated `sidebar.items`
  must therefore also be scope-filtered before P3 previews rely on it.
- Compat layer renders `Note/Info/Tip/Warning/Danger`, `Update`, badges, `SettingsInfoBlock`,
  `VersionHistory`, `Image`; `RunnableCode` hydrates as an island (`client:visible`).
- Markdown twins, `llms.txt`, per-section `llms.txt`, sitemap and robots are emitted with base-correct URLs.

## MDX compatibility tail (Sätteri/mdxjs-rs vs Mintlify's micromark)

`bin/measure/mdx-compile-check.ts` compiles every MDX file with the site's Sätteri
configuration in a few seconds. After the fixes below, **3,671/3,671 English + snippet
files and 2,580/2,580 files in each of the 8 locale trees compile**, with no content
changes. Fixes live in `src/plugins/vite-mintlify-snippets.ts` (`normalizeMdx`):

- Math: `$$ ... $$` and `$...$` are parsed as JSX expressions unless Sätteri's `math`
  feature is on (11 reference pages). Enabled in `src/plugins/satteri-features.ts`; KaTeX
  rendering of the resulting math nodes is a later step.
- ESM blocks end at a blank line in mdxjs-rs, unlike micromark, which keeps parsing while
  the program is incomplete. Two consequences: an `import` block glued to the next line
  swallows that line (`#` from a heading, `<Info>` opener), and `export const X = () => (
  <>...</>)` components with blank lines inside break (`snippets/clickpipes/_iam_authentication.mdx`).
  The plugin inserts the blank line after imports and drops blank lines inside open ESM blocks.
- Partials (`_snippets/*.mdx`, `snippets/*.mdx`) use components they never import, because
  Mintlify inlined them into the importing page. `bin/gen-import-index.ts` derives a
  name -> specifier index from the content's own imports (125 names) and the plugin injects
  the missing imports; Mintlify built-ins come from `src/components/compat/globals.ts`.
- `import Content from ...` (8 pages) collides with Astro's MDX wrapper identifier; renamed.
- Shiki language aliases for `response`, `result(s)`, `SQL`, `Python`, `python3`, `test`,
  `code`, `capnp`; one content typo (`sq;`) to fix.

## Nimbus internals worked around (candidates for upstream PRs)

1. **Sub-path deployments** (cloudflare/nimbus#105, #78): sidebar hrefs, breadcrumb hrefs, section
   tabs, `getIndexedEntries` URLs, favicon and `shiki.css` hrefs ignore Astro `base`.
   Worked around in owned code: `src/lib/base.ts` (`withBase`, `stripBase`, `prefixSidebarTree`),
   applied in `src/pages/[...slug].astro`, `src/pages/[locale]/[...slug].astro`,
   `src/components/Header.astro`, the llms/markdown routes, and a vendored
   `src/components/NimbusHead.astro` (two hrefs).
2. **Heading attributes** (`{#id}`): Sätteri `headingAttributes` is off by default and not exposed by
   the `nimbus()` integration; we pass our own `satteri({ features: { headingAttributes: true } })`
   processor and re-add `tableScroll()`. Upstream: expose `markdown.features`.
3. **`title` required**: the homepage and section `README.mdx` files have none; `title` made optional
   in the schema, the routes fall back to `sidebarTitle` or the id.
4. **Frontmatter collisions** neutralised in `src/content.config.ts`: `sidebar` (Docusaurus strings /
   `false`), `draft` (would drop a page), `mode` (`wide` plus stray values). Nimbus's strict
   frontmatter is disabled (`strictFrontmatter: false`).
5. **`validateMdx: false`**: the PascalCase pre-check does not know MDX snippets imported as components.
6. **Root `index.mdx`** excluded from the collection (inline React with hooks; native homepage in P4).
7. **Astro's glob loader honours `slug` frontmatter**: `pathId` derives ids from the path only, and
   keeps `folder/index` (Mintlify does not collapse index pages).
8. **Owned homepage and fallback links**: constructing links with
   `import.meta.env.BASE_URL` omitted a separator for three homepage cards,
   while the 404 and version fallback links bypassed `/docs`. All now use
   `withBase`.
9. **Scoped include patterns**: `DOCS_INCLUDE` is comma-delimited, but glob
   brace alternatives also use commas. `splitPatterns` now preserves commas
   inside brace groups, so a scope such as `{install,guides}/**/*.mdx` is one
   pattern rather than two invalid fragments.

## Build isolation

Two Astro builds sharing `dist/` and the content-layer cache corrupt each other
(`Cannot find module dist/.prerender/chunks/...`). During P0 an unidentified process (not
the listed peer sessions) ran three builds in this checkout at 12:59-13:01. `astro.config.ts`
now honours `DOCS_OUT_DIR` and `DOCS_CACHE_DIR`, so every run (CI shards, parallel
sessions) can build in isolation; the check scripts take the output directory as argument.

## P1 progress (2026-09-03)

- Full `reference/` tree (1,326 pages) builds in 59 s, peak RSS 2.0 GB, 9,776/9,776 anchors,
  all URLs under `/docs` after the raw-JSX `<img>` fix.
- `bin/gen-sidebar.ts` converts `docs.json` + 33 English `navigation.json` files into
  `src/generated/sidebar.items.json` (5 tabs, 2,678 page links) and writes
  `reports/nav-vs-disk.md`: 65 navigation entries without a file (Cloud API operation strings,
  ClickStack API), 25 orphan pages, 207 groups whose authored order differs from alphabetical
  disk order, 3 skipped constructs (openapi x2, sourceRef).
- Page weight: with the section tree rendered inline, a settings page was 1.38 MB, 90% of it the
  desktop rail (621 KB) plus the mobile drawer duplicate (622 KB), ~470 bytes per link. Fix:
  collapsed groups off the active path render a placeholder and fetch a prebuilt fragment
  (`/_nav/<key>/`, Astro page partial) on first expand (`src/lib/sidebar-lazy.ts`,
  `src/pages/_nav/[...key].astro`, `src/components/ui/sidebar/SidebarChildren.astro`,
  `sidebar-lazy.client.ts`). Measured by `bin/measure/page-weight.ts`.
- Undefined-at-render components found by the `--refs` mode of the compile checker: `View`,
  `Visibility`, `CodeBlock` added to the compat layer; inline `export const` components in 4 hub
  pages get no-op hook shims (`src/components/compat/react/hook-shims.ts`) so they render once.
- A prose sentence "import ... from" made the declaration detector believe a component was
  imported; `declared()` is now anchored to real import statements and declarations.

## P1 baseline measurements (English, 2026-09-03, laptop M-series)

| Metric | Value |
|---|---|
| Build (2,657 pages + 344 nav fragments + md twins) | 1 min 35 s astro build, 2 min 19 s wall incl. prebuild/postbuild |
| Peak RSS | 2.2 GB (8 GB heap allowed) |
| Output files after postbuild | 8,398 (limit 100,000) |
| Anchor parity | 19,745 / 19,745 |
| Base-path check | 0 site-relative URLs outside /docs |
| URL parity vs live Mintlify sitemap (English) | 2,627 / 2,771 (94.8%); misses = 103 Cloud/ClickStack API pages (P5), 80 private-docs pages under two prefixes (P3), 1 stale sitemap URL |
| Page weight | median 123 KB, p95 183 KB, max 1.7 MB (OSS changelog content) |
| Sidebar bytes | ~70 KB average, mobile drawer 1 KB (cloned from the rail on open) |
| Redirect rules | 14,638 (13,607 from redirects.json, 4 splats, 1,031 index-page rules) + 17 fragment redirects for the client |

Index pages: Mintlify serves `folder/index.mdx` at both `/folder` and `/folder/index` and its
sitemap canonical is `/folder`; the build emits `/folder` and redirects `/folder/index`.

## Edge findings

- `llms-full.txt` for English alone is 26.3 MB, over the 25 MiB Workers asset limit (and it
  would include every locale). Replaced by per-section corpora `/<section>/llms-full.txt`
  (and `/<locale>/<section>/llms-full.txt`) with index files at the old URLs
  (`src/lib/corpus.ts`); `bin/postbuild.ts` fails the build on any file over 25 MiB.
- The site is nested under `dist/docs` by postbuild so Workers static-asset paths equal the
  request paths; `__redirects` sits above it and is excluded from assets (`.assetsignore`).
- `docs/worker/index.ts` serves all 14.6k redirects with the owned
  `worker/redirects.ts` evaluator, markdown negotiation (`Accept: text/markdown`, `index.md`,
  `.mdx` -> `.md`) and a markdown-aware 404. `redirects-in-workers` delegates to the platform
  parser, whose 200 dynamic-rule cap stopped it reading the file after 469 rules; it is not used
  at runtime. A tail static rule, its Markdown variant, and a splat rule all returned 301 locally.
- Worker validation on 2026-09-03: `wrangler deploy --dry-run` (Wrangler 4.128.0) packages
  successfully with the `ASSETS` binding. A local Worker smoke test returned HTML 200 for a
  canonical page, Markdown 200 with `Vary: Accept`, a 301 legacy redirect, the corresponding
  Markdown redirect ending in `index.md`, and a Markdown 404. Wrangler warns that some static
  redirects follow splat rules and could be reordered for speed; behavior is correct.

## Remote repositories (P3)

`remotes.json` + `bin/fetch-remotes.ts` copy another repository's docs into a mount directory
under `docs/` (contents gitignored; committed `images/` kept), so the primary collection builds
them at their current URLs. `ClickHouse/airgapped-docs` (40 pages) mounts at
`products/clickhouse-private`, and `bin/gen-sidebar.ts` expands the Mintlify `sourceRef`
group from the remote's own `docs.json`. Locally the fetcher uses a checkout path
(`DOCS_REMOTE_CLICKHOUSE_PRIVATE_PATH`); in CI a tarball with `GH_TOKEN`. Private remotes are
skipped, and reported, when neither is available (fork previews).

## Preview scope (P3)

Preview builds read the scope Praktika commits as `docs/.preview-scope.json` (see
`ci/jobs/scripts/docs/nimbus_scope.py`), translated to `DOCS_LOCALES` and `DOCS_REFERENCE=off`.
With `DOCS_REFERENCE=off` the `reference/` section is excluded from the collections and the
generated sidebar links it to production (`https://clickhouse.com/docs/reference/...`), so a
preview never needs the `clickhouse` binary and builds in a fraction of the time.

`Docs check (Nimbus)` is registered in the PR workflow and its scope smoke test verifies the
English-only, one-locale, and all-locale cases. It runs source checks now and skips the remote
preview step until the Workers Builds API token, account id, trigger UUID, and Workers subdomain
are provisioned. Its poll timeout is 20 minutes, matching the Workers Builds cap.

### External provisioning required before P3 can finish

- Create/connect the `clickhouse-docs` Workers Builds project for this repository, rooted at
  `docs/`, with production on `master` and previews for `preview/*` branches. Configure the
  20-minute build limit, Node 24, 8 GB Node heap, build caching, the production deploy command,
  and preview aliases.
- Create the `clickhouse-docs-artifacts` R2 bucket and grant the production build its read-only
  credentials for the later reference bundle. Preview builds must not receive those credentials.
- Add the Workers Builds API token, Cloudflare account id, preview trigger UUID, and Workers
  subdomain to the Praktika environment as `CLOUDFLARE_BUILDS_API_TOKEN`,
  `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_DOCS_PREVIEW_TRIGGER_UUID`, and
  `CLOUDFLARE_WORKERS_SUBDOMAIN`. The API token needs only the Builds permissions required to
  start, list, cancel, and inspect the `clickhouse-docs` builds.
- Add the `DOCS` Service Binding from the production website Worker to `clickhouse-docs`, then
  deploy the website Worker with `docs_nimbus_cutover` still disabled. A live preview requires a
  `?nimbus_preview=1` request to reach the binding, followed by a rollback rehearsal.

## P2 status (locales)

- One collection per locale, named by the lowercase URL segment (`pt-br`, as Mintlify serves
  it), built from the build scope. Locale pages render their own rail from
  `src/generated/sidebar.items.<locale>.json` (translated labels, `/docs/<locale>/` links, lazy
  fragments under `/docs/nav/<locale>/`) and their own header tabs; `html lang`/`dir`, hreflang
  alternates (English, x-default, every active locale) and the canonical override for
  untranslated pages are plumbed through `BaseLayout`/`DocsLayout`/`NimbusHead`.
- Untranslated pages render the English entry at the locale URL with a localized
  "not translated yet" notice and a canonical link to the English page (88 such pages for `es`).
- Nimbus's agent index only sees collections declared statically in `content.config.ts`, so the
  locale twins, `llms.txt` and corpora read the collections directly (`src/lib/corpus.ts`).
- English + Spanish (verified 2026-09-03 13:47): 6,095 outputs in 1 min 54 s (2 min 44 s wall
  incl. pre/postbuild), peak RSS 4.9 GB, 14,139 files; Spanish rail translated with `/docs/es/`
  links and 344 lazy fragments, 2,576 Spanish markdown twins, `/es/llms.txt`, per-section
  Spanish corpora (reference 9.9 MB); anchors 19,758/19,758; 0 base-path misses; 128 fallback
  pages. Memory is the number to watch for the 9-locale build (Workers Builds has 8 GB).
- Full nine-locale build (verified 2026-09-03): 27,439 routes in 10 min 13 s and 53,676 files
  after postbuild, below the 100,000 Workers asset cap. It preserved 19,851/19,851 explicit
  anchors across 27,438 HTML pages and all 3,693,151 site-relative URLs remained below `/docs`.
  The build log does not include peak RSS, so the first Workers Builds run must record it.
- `LocaleSwitcher.astro` is wired into the documentation header. It preserves the current
  document path while switching between English and the active build's locale prefixes, so a
  scoped preview never links to a locale that was not built. The native locale homepage fallback
  emits all eight `/docs/<locale>/` roots (with `lang` and Arabic `dir="rtl"`); P4 will replace
  its English body with the translated homepages.
- Full sitemap parity is 22,963/23,931 (95.96%). Each locale now resolves 2,542/2,645 (96.11%);
  the 103 misses per locale are the deferred Cloud and ClickStack API endpoints. English has the
  previously measured 144 misses: 65 Cloud API pages, 40 private-doc pages, 38 ClickStack API
  pages, and one stale sitemap URL. These are P3/P5 work, not locale-routing omissions.
- A scope containing only English content has empty locale collections. Astro then emits warnings
  for locale `llms.txt`/`llms-full.txt` routes because there is no translated corpus. Production
  (and scopes with translated files) is unaffected; P3 preview scoping should omit those routes
  for empty collections or build their indexes from the English fallback set.

## Website Worker (P3)

Worktree `/Users/sstruw/Desktop/clickhouse-website-worker-nimbus`, branch `nimbus-docs-binding`
(from `origin/main`, uncommitted): `src/nimbus-docs.ts` dispatcher mirroring the Mintlify
cutover (`?nimbus_preview=1` cookie, `config.docs_nimbus_cutover`, shared docs CSP, cookie
banner injection, immutable caching for `/docs/_astro/*`), `env.ts` `DOCS?: Fetcher`,
`handler.ts` routing `/docs` through it, and a commented `services` binding in `wrangler.toml`.
With the binding absent the dispatcher falls back to the Mintlify path, so it is safe to merge
before the `clickhouse-docs` Worker exists. Coordinated with the session working in that repo.

## P4 status (search, 2026-09-03)

- `src/scripts/inkeep.ts` replaces the dormant Pagefind dialog with the hosted Inkeep
  search-only modal. It binds Inkeep directly to the Nimbus `[data-search-trigger]`
  controls (including its `Cmd-K` / `Ctrl-K` behavior), so no Mintlify element IDs or
  global customisation files are required.
- The migration retains the existing public browser-key selection, canonicalises results from
  the old preview host to `https://clickhouse.com/docs/`, preserves the five top-level result
  tabs and Docs sub-area filters, follows the site dark-mode class, and hides Inkeep chat so
  Kapa remains the sole Ask AI surface. It intentionally marks search unavailable if the hosted
  script cannot load rather than substituting an unrelated local index.
- `src/scripts/kapa.ts` loads the existing Kapa Ask AI widget with its stable GA-derived visitor
  identifier, hidden launcher, public configuration, and iOS 16.4 compatibility guard.
  `AskAiButton.astro` adds a native header control which opens Kapa; an early click is held until
  the asynchronously loaded widget is ready.
- `src/scripts/anchor-redirects.ts` reads the generated 17-entry fragment redirect table and
  performs exact client-side replacement redirects, preserving query strings before the target
  fragment. This is deliberately limited to fragment routes, which never reach the edge Worker.
- `src/scripts/settings-anchor-redirects.ts` replaces the legacy continuous page observer. It
  code-splits the four generated settings tables and loads one only when a matching legacy
  settings index URL has a hash, including under a locale prefix; its destination and
  case/punctuation matching preserve the legacy behavior.
- `NavbarCta.astro` replaces the legacy DOM-injected signup CTA with a responsive native header
  action and retains its Galaxy event attributes. `PageActions.astro` now links directly to the
  repository's documentation issue form, with the rendered page pathname carried in its `page`
  field; no mutation observer is needed.
- `src/scripts/webterminal.ts` emits the established terminal tray as a same-origin hashed build
  asset and loads it from Nimbus, without Mintlify's global script injection. The artifact retains
  its keyboard shortcut, persisted height, iframe origin checks, and narrow-viewport behavior;
  an interactive browser smoke test remains part of visual review.
- `bin/gen-galaxy-script.ts` produces a same-origin Galaxy asset from the established transport,
  changing only the host classification so Workers Builds preview URLs use the preview control
  plane. `src/scripts/galaxy.ts` loads that hashed asset, preserving the event envelope,
  attribution, link instrumentation, and existing `data-galaxy-event` controls.
- Build-time SQL highlighting now uses `@clickhouse/lexer`, pinned to repository commit
  `d5ba7f654c70c0bdb56d8de381e42f26dae6d36c` because the advertised npm package was not yet
  available from the public registry. `src/plugins/shiki-clickhouse-sql.ts` initializes its WASM
  module once during Astro configuration, re-tokenizes every SQL block synchronously, and emits
  semantic palette classes before HTML is written. The prior browser lexer/highlighter is not
  included in the Nimbus output. The keyword table remains generated from the established
  highlighter because ClickHouse's lexer deliberately classifies keywords as bare words.
- English production build after the SQL lexer change: 3,047 pages in 54.70 s; postbuild completed
  with 8,499 output files. Rendered HTML contains `ch-sql-kw` and identifier/function token
  classes from the ClickHouse lexer. Remaining P4 work is visual review and the locale homepages;
  live analytics observation needs an externally provisioned preview.

## P4 homepage fidelity correction (2026-09-03)

- The initial native four-card summary did not preserve the authored homepage closely enough.
  It is superseded by `bin/gen-homepage-react.ts`, which materialises the existing English and
  eight locale `index.mdx` JSX bodies as React islands. This retains every authored card, use-case
  and product section, inline imagery, analytics attributes, search and Ask AI actions, and
  localized copy without changing the Mintlify sources before cutover.
- The English root and every `/docs/<locale>/` root now use that exact generated body together
  with the homepage logo and primary navigation. Server-rendered links retain the deployment base
  and their active locale; `pt-BR` is emitted at the canonical `/pt-br/` route.
- The focused Arabic build rendered 6,806 pages in 3 min 26 s and verified the right-to-left
  homepage, Arabic copy, Inkeep trigger, and locale-prefixed links. The final nine-locale
  production build generated all nine islands and rendered 28,149 pages in 14 min 12 s
  (54,406 postbuild files), with no homepage errors. Static samples from every locale confirmed
  the full card layout, logo, search action, and localized primary navigation destinations.
- Development hydration initially exposed a React runtime mismatch that static rendering cannot
  observe: generated JSX compiled to `jsxDEV`, while Vite's optimized React development-runtime
  dependency resolved to its production implementation where that helper is undefined.
  `astro.config.ts` now forces the automatic `jsx` runtime in both modes. A direct transform
  check confirmed no `jsxDEV` calls, and the following English production build rendered 3,757
  pages in 2 min 04 s (9,222 postbuild files) without homepage errors.
- Live-site comparison of the top navigation established that the authored menus are compact,
  one-column hover/focus panels, not landing-page links or multi-column mega menus. The homepage
  header now uses the original light/dark logo assets and `HomepageNavigation.astro` renders the
  live first-level menu labels, Solutions section dividers, authored icon assets, locale-aware
  destinations, mouse hover, keyboard focus, click toggle, and Escape dismissal. The validation
  build rendered 3,757 pages in 1 min 20 s (9,224 postbuild files) without menu errors.
- The same live comparison showed that homepage chrome uses GitHub stars, Sign in, and Get
  Started on the right, while Search and Ask AI belong in the hero. `HomepageNavbarCta.astro`
  now reproduces those controls and refreshes the public star count from GitHub, with the
  established fallback count if the request fails. The final English build rendered 3,757 pages
  in 1 min 22 s (9,224 postbuild files) without homepage or header errors.
- The live-style primary navigation is now a shared header component, rather than a homepage-only
  branch. It remains visible on every desktop documentation route, with the active Home state
  limited to the homepage and the existing sidebar, locale selector, search, Ask AI, and page
  controls retained on articles. The English validation build rendered 3,757 pages in 1 min 21 s
  (9,224 postbuild files); an emitted non-home article contains all four hover/focus menus and
  no active Home marker.
- Search and Ask AI now sit next to each other directly above the sidebar filter as compact
  Mintlify-style documentation tools: a flexible 36px `Search…`/shortcut control and a fixed
  `Ask` action share the original subtle surface, border, hover, and dark-mode contrast treatment.
- The documentation rail now scopes itself to the current top-menu entry, not merely the current
  top-level tab. Thus `Solutions > ClickHouse Cloud` does not render the Managed Postgres,
  ClickStack, `chDB`, or Kubernetes branches; Database, Integrations, and Resources are scoped
  the same way. English and translated routes share this generated-tree implementation, with
  their existing lazy fragment keys retained.
- The shared Nimbus tokens and primitives are now re-based on the established Mintlify visual
  system, using [the Astro reference prototype](https://github.com/ClickHouse/ClickHouse/pull/115770)
  as an additional implementation reference: `#151515` canvas, elevated/code surfaces,
  `#2b2b2b` structural rules, muted `#9f9f9f` text, yellow dark-mode links/actions, 304px rail,
  768px article measure, and underline-driven primary navigation. Sidebar and menu controls use
  those same values rather than stock Nimbus pills. The initial shared-cache build hit the known
  deferred-content race; the isolated-cache validation succeeded with 3,769 pages and 9,308
  postbuild files.
- The desktop docs shell now matches the reference navigation geometry rather than only its
  colors: a fixed 304px sidebar owns the logo/theme region, the 48px header begins after that
  rail, primary menu tabs are right-aligned and underline-active, and the existing GitHub stars,
  Sign in, and Get Started controls form the header's right edge. The regular page title is no
  longer incorrectly rendered in the desktop top bar.
  They retain the existing triggers and keyboard behavior, remain in the cloned mobile drawer,
  and stay in the header only for pages without a sidebar.
- The final live-site parity pass measured and reproduced the homepage and article shells at the
  same desktop viewports. Homepage width, vertical spacing, muted category icons, search, cards,
  header actions, and the two-column Solutions menu now match the live styling. Article rails use
  the source navigation icons, Mintlify's authored expanded state, fixed first-level section
  headings, and a two-entry fallback outline for raw HTML headings. The sidebar filter remains
  directly below the side-by-side Search and Ask controls as an intentional project requirement.
  The isolated validation build rendered 3,769 pages and produced 9,308 files without errors or
  new warnings from these changes.

## P5 changelog collection (2026-09-03)

- `bin/gen-changelog-entries.ts` materialises the existing `changelogs/` corpus into the
  ignored `.remote/changelog/` source tree: 689 individual OSS release entries plus 18 individual
  ClickHouse Cloud updates split from the current Cloud page's `<Update>` blocks. Existing
  `resources/changelogs/**` pages remain unchanged for URL parity until cutover redirects are
  deliberately added.
- `content.config.ts` registers the typed `changelog` collection (`title`, `description`,
  `date`, `products`, optional release channel, `hidden`). `src/pages/changelog/**` supplies an
  all-products index, Cloud and OSS product indexes, per-entry pages, and RSS feeds at
  `/changelog/rss.xml`, `/changelog/cloud/rss.xml`, and `/changelog/oss/rss.xml`.
- Historical OSS files are Markdown rather than MDX. The materialiser encodes literal braces and
  angle brackets only in the generated copies, preserving SQL macros, log lines, and old embedded
  HTML as text without changing the Mintlify source corpus. Version filenames provide the release
  month sort key; because the old files have no publication day, the generated date uses the first
  day of that month rather than implying an exact release date.
- Validation: the full English build generated all 707 entries and rendered 3,757 pages in
  58.04 s (9,213 postbuild files). Static output includes the root index, both product feeds, a
  representative 2020 archive entry, and the current Cloud update. No P5 errors or warnings;
  the remaining build advisories are the existing Vite deprecated-esbuild warning, the >500 kB
  chunk advisory, and 144 plaintext Shiki fallbacks.

### OpenAPI capability gap (validated 2026-09-03)

The pinned `@cloudflare/nimbus-docs` 0.11.0 package has no public `api` configuration field and
contains no OpenAPI implementation. The P5 design cannot therefore use the planned native
`api: [...]` collection mechanism in this version. The existing generated Cloud API MDX pages
continue to render through the primary compatibility collection at their current URLs, but a
spec-driven replacement requires either an upstream Nimbus release or an owned OpenAPI renderer.
This is a framework gap, not an API-source or route-parity failure; Cloud's 1.2 MB pinned spec and
the ClickStack OpenAPI source are both present in the current Mintlify configuration.

## Locale tree quirks

- Each locale tree has a stale `concepts/features/interfaces/odbc.mdx` next to
  `odbc/index.mdx` (the page moved into a folder; the translation of the old file was never
  removed). Both collapse to the same id, so `src/lib/stale-siblings.ts` excludes such siblings
  from the collections; the folder index is what navigation references and what English has.
- Locale navigation only "misses" the 147 Cloud API entries (per-operation stubs and operation
  strings are English-only, excluded from translation), see `reports/nav-vs-disk.<locale>.md`.
- Locale trees lag English by 88 to 128 pages (English fallback pages fill the gap).

## All nine locale trees: partial measurement

Two attempts to build all locales (`DOCS_LOCALES=all`, ~24,300 HTML pages) were stopped
externally. The second (nice 10, 8 GB heap) got through: content sync 22 s, Vite bundling
2 min 52 s, then rendering at ~115 HTML pages/s plus 20,608 markdown twins and 3,101 nav
fragments in the first 48 s, before being stopped at 13:57:57 with 5,472 of the HTML pages done.
Extrapolated wall time on this laptop: 8 to 9 minutes, i.e. inside the 20-minute Workers Builds
cap with margin, but on a 4-vCPU runner expect roughly double. Peak memory was not captured
(the run was killed before `time` reported); English alone peaked at 2.2 GB and English + Spanish
at 4.9 GB, so the 8 GB of a Workers Builds runner is at risk and the first full run there must
measure it. Fallbacks if it does not fit: per-locale-group build shards merged in postbuild, or
reducing per-entry memory in the content layer (rendered HTML twins are the bulk).
Command: `DOCS_LOCALES=all DOCS_OUT_DIR=tmp/dist-all DOCS_CACHE_DIR=tmp/astro-cache-all NODE_OPTIONS=--max-old-space-size=8192 pnpm build`.

## Open items carried into P1

- Sidebar generated from `docs.json` + `navigation.json` (currently Nimbus's filesystem sidebar).
- Legacy Mintlify CSS (`_site/styles.css`) not loaded yet; badges/images lack their styles.
- Mermaid: plugin and lazy renderer wired; `mermaid` dependency to be added.
- Markdown twins still contain `{/* AUTOGENERATED_* */}` comments? (verify) and Mintlify-specific JSX.
