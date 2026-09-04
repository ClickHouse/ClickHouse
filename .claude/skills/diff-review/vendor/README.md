# Pinned UI dependencies

`manifest.json` is the source of truth: one entry per asset the review UI
imports, each with the sha256 of the module and the URL it can be fetched from.
`../assets.mjs` resolves them — `vendor/`, then `~/.cache/diff-review`, then the
network — and checks every asset's hash against the manifest before the server
serves it, cache hits included. A mismatch, or a fetch that fails, stops the
server; unverified bytes are never handed to the browser. So a compromised CDN
cannot exfiltrate the diff under review (which includes uncommitted local
changes) — the worst it can do is take the review offline.

Only the two small kinds of file below are committed. The `@pierre/diffs` bundle
is 10.8 MB (1.9 MB gzipped) of build output and is deliberately **not** in git:
keeping it there meant a permanent copy in history for every upgrade. It is
downloaded once, verified, and cached, so it costs a network fetch on a machine
that has never run a review.

## `@pierre/diffs`

Self-contained ESM bundle of [`@pierre/diffs`](https://www.npmjs.com/package/@pierre/diffs)
with all its dependencies inlined, including the base64-embedded oniguruma wasm
used by shiki. Served at `/vendor/pierre-diffs.mjs`; cached gzipped, and served
gzipped to browsers that accept it.

- Source: `https://esm.sh/@pierre/diffs@<version>/es2022/diffs.bundle.mjs`
  (the `?bundle` build of `https://esm.sh/@pierre/diffs@<version>`), served
  `cache-control: immutable`.
- Version and sha256: `manifest.json`.
- `1.3.5` was taken for `renderHeaderFilenameSuffix`, the header slot that puts a
  control immediately after the displayed filename. Audited on the way in from
  `1.2.12`: its external imports are the same two polyfills `1.2.12` used, so the
  set of files below is unchanged.

## `node_*.mjs`

esm.sh's browser polyfills for node built-ins, committed here because they are a
few KB each and never change. The bundle imports them by absolute path
(`/node/process.mjs`, `/node/buffer.mjs`; `process` transitively pulls `events`
and `tty`, and `events` pulls `async_hooks`), so the server serves them at
exactly those specifiers. Audited: none of them import, fetch, or open
connections to anything. Sources are `https://esm.sh/node/<name>.mjs`; hashes are
in `manifest.json`.

## Working offline

The cache makes the download a one-off, but a machine that will never have a
network needs the bytes put there another way. Either fill the cache ahead of
time, on the machine or by copying `~/.cache/diff-review` over:

```bash
node ../server.mjs --prefetch          # DIFF_REVIEW_CACHE=<dir> to point it elsewhere
```

…or drop the file into this directory under the manifest's `name`, where it is
found first and the network is never touched (it is gitignored, so it stays out
of history):

```bash
gzip -9 -c pierre-diffs-1.3.5.mjs > vendor/pierre-diffs-1.3.5.mjs.gz
```

## Upgrading

```bash
curl -sL 'https://esm.sh/@pierre/diffs@<version>/es2022/diffs.bundle.mjs' -o bundle.mjs
sha256sum bundle.mjs && wc -c bundle.mjs   # -> manifest.json: sha256, bytes, name, urls
# audit: apart from the /node/*.mjs polyfill imports, the bundle must not
# import, fetch, or open connections to anything external:
grep -o '\(from\|import\) *"[^"]*"' bundle.mjs | sort -u
```

Update `manifest.json` and nothing else: the cache is keyed by hash, so the new
pin fetches into a new file instead of colliding with the old one, and a revert
finds the old file still cached. If the audit turns up a new polyfill import, add
it to `manifest.json` and commit it here.
