# Vendored dependencies

## `pierre-diffs-1.2.12.mjs.gz`

Self-contained ESM bundle of [`@pierre/diffs`](https://www.npmjs.com/package/@pierre/diffs)
`1.2.12` (with all its dependencies inlined, including the base64-embedded
oniguruma wasm used by shiki). Vendored so the review UI never loads code from a
CDN at runtime — a compromised CDN could otherwise exfiltrate the diff under
review, which includes uncommitted local changes.

- Source: `https://esm.sh/@pierre/diffs@1.2.12/es2022/diffs.bundle.mjs`
  (the `?bundle` build of `https://esm.sh/@pierre/diffs@1.2.12`)
- sha256 of the uncompressed `.mjs`:
  `73d1514f14b64925ee47f8afbe885fabd104868c4e3f18e59c3d9a8acf24122a`
- Served by `server.mjs` at `/vendor/pierre-diffs.mjs`.

## `node_*.mjs`

esm.sh's browser polyfills for node built-ins. The bundle above imports them by
absolute path (`/node/process.mjs`, `/node/buffer.mjs`; `process` transitively
pulls `events`, `tty`, and `events` pulls `async_hooks`), so `server.mjs` serves
them at exactly those specifiers. Audited: none of them import, fetch, or open
connections to anything.

| file | source | sha256 |
| --- | --- | --- |
| `node_process.mjs` | `https://esm.sh/node/process.mjs` | `79e7646e87709989f575ea4ce02e0877bc9303081567b1c0d412527917ae9e91` |
| `node_buffer.mjs` | `https://esm.sh/node/buffer.mjs` | `64fb61aa5f48644d685f9ceabedba60ea6b5d6ce03dac1943e863d00d9e574f3` |
| `node_events.mjs` | `https://esm.sh/node/events.mjs` | `4c6150b88c1444aa1fe9331013e3f37eda9836206f629f7f3ae8f3743dd90fa8` |
| `node_tty.mjs` | `https://esm.sh/node/tty.mjs` | `c66ff4b406bad449bfb2ced355f15badf16f4d9e035d2d300e33b5aeee64e3be` |
| `node_async_hooks.mjs` | `https://esm.sh/node/async_hooks.mjs` | `b7862dbfba8bbbca956f19e4e08280b529e4b27468779775a9093aef8c92dc1d` |

## Regenerating or upgrading

```bash
curl -sL 'https://esm.sh/@pierre/diffs@<version>/es2022/diffs.bundle.mjs' -o pierre-diffs-<version>.mjs
sha256sum pierre-diffs-<version>.mjs   # record here
# audit: apart from the /node/*.mjs polyfill imports, the bundle must not
# import, fetch, or open connections to anything external:
grep -o '\(from\|import\) *"[^"]*"' pierre-diffs-<version>.mjs | sort -u
gzip -9 pierre-diffs-<version>.mjs
# refetch each polyfill the bundle (transitively) imports, re-audit, update the
# table and the VENDOR_FILES map in server.mjs
```
