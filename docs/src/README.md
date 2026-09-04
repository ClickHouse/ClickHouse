# Nimbus site (Project Nimbus POC)

Astro 7 + `@cloudflare/nimbus-docs` build of the ClickHouse docs, living next to the
Mintlify content it renders. Content files are untouched: a compatibility layer makes the
Mintlify-flavoured MDX build (see `src/plugins/vite-mintlify-snippets.ts` and
`src/components/compat/`). Findings and measurements: `../reports/nimbus-poc-notes.md`.

## Commands (run in `docs/`)

| Command | What it does |
|---|---|
| `pnpm install` | Node 24, pnpm 10. |
| `pnpm build` | `prebuild` (compat wrappers, import index, sidebar from docs.json/navigation.json) -> `astro build` -> `postbuild` (prune `.mdx` twins, rebase stray URLs, generate `__redirects`, nest under `dist/docs`, 25 MiB gate). |
| `pnpm dev` | Astro dev server (`/docs/...`). |
| `pnpm check:mdx` | Compiles every MDX file with Sätteri and reports undefined components; seconds, no build. |
| `pnpm measure` | Page weight, anchor parity, base-path check, URL parity vs the live Mintlify sitemap (needs a nested build in `$DOCS_OUT_DIR`). |
| `pnpm preview:cf` | `wrangler dev` on the docs Worker (`worker/index.ts`) serving `dist/`. |
| `node bin/fetch-remotes.ts` | Pulls remote-repo docs (`remotes.json`) into their mount directories. |

## Environment variables

| Variable | Effect |
|---|---|
| `DOCS_INCLUDE` | Comma-separated globs restricting the English collection (spikes, scoped previews). |
| `DOCS_LOCALES` | `all` or a comma list (`es,fr`) of locale trees to build; unset = English only. |
| `DOCS_OUT_DIR`, `DOCS_CACHE_DIR` | Isolated output and cache directories (parallel builds never share `dist/`). |
| `DOCS_REMOTE_CLICKHOUSE_PRIVATE_PATH` | Local checkout of `ClickHouse/airgapped-docs` for `fetch-remotes` (CI uses `GH_TOKEN`). |
| `NODE_OPTIONS=--max-old-space-size=8192` | Recommended for full builds (peak RSS ~3 GB). |

## Layout

- `astro.config.ts`: Nimbus config, Sätteri processor (heading attributes + math), URL rebaser, mermaid, compat Vite plugin.
- `src/content.config.ts`: `docs` (English, path-derived ids) and one collection per locale (`es`, `pt-br`, ...).
- `src/pages/[...slug].astro`, `src/pages/[locale]/[...slug].astro`: page routes (locale pages fall back to English).
- `src/pages/nav/[...key].astro`: lazy sidebar fragments; `src/lib/sidebar-lazy.ts`.
- `src/pages/**/llms*.txt.ts`, `**/index.md.ts`: agent surfaces (chunked corpora in `src/lib/corpus.ts`).
- `src/components/compat/`: Mintlify component names on Nimbus components; `react/` shims for snippet JSX.
- `bin/`: generators and measurement scripts; `worker/`: Cloudflare Worker; `wrangler.jsonc`.
- `src/generated/` (gitignored): sidebar items, import index, island wrappers.
