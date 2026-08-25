---
description: 'S3 key layout and on-disk text-object formats used by the content-addressed storage (CAS) MergeTree disk backend.'
sidebar_label: 'Storage layout'
sidebar_position: 2
slug: /antalya/cas/architecture/storage-layout
title: 'CAS Architecture — Storage Layout'
doc_type: 'reference'
---

# CAS architecture — storage layout {#storage-layout}

Every key in a pool is built by one class, `Cas::Layout` (`Formats/CasLayout.h`), which owns
exactly the pool prefix. Every persisted object opens with a one-line JSON envelope header, and
control-plane bodies are JSON Lines — one JSON object per line, sorted where the object is a log
or a set of entries (`Formats/README.md`; see [Envelope format](#envelope-format) below for which
parts are a single JSON object versus JSON Lines versus raw payload bytes). The format is
deliberately this plain: any object can be fetched and read with ordinary line-oriented tools
while debugging, and a new field is additive — a tolerant reader skips it — so the format evolves
without a migration.

## Key table {#key-table}

All key patterns are shown under the pool prefix. A **namespace** is the opaque per-table string
under which one `MergeTree` table's part manifests and ref history live: for a live table it is
the table's canonical disk path (`store/<xx>/<uuid>`, `@cas@`-marked) prefixed by the owning
server's `server_root_id`, and a `FREEZE` backup gets its own
`<server_root_id>/shadow/<backup>/…` namespace under that same root; `Cas::Layout` only validates a
namespace's shape and never interprets its contents.

| Key pattern | Object | Codec | Writer |
|---|---|---|---|
| `_pool_meta` | pool identity + floors | `cas_pool_meta` | pool create/admit |
| `blobs/<algo>/<hex[0:2]>/<hex>` | blob envelope + payload | `cas_blob` | uploads |
| `blobs/<algo>/<hex[0:2]>/<hex>.meta` | blob freshness sidecar | `cas_blob_meta` | dedup/GC |
| `cas/ns/stream/<life_id>/_log/<epoch-hex>-<seq-hex>.zst` | ref transaction log | `cas_ref_log` | writer commit path |
| `cas/ns/stream/<life_id>/_snap/<epoch-hex>-<seq-hex>.zst` | complete ref table snapshot | `cas_ref_snap` | writer/GC fold |
| `cas/ns/state/<life_id>/_ckpt` | mutable per-life checkpoint | `cas_ref_ckpt` | writer/GC fold |
| `cas/ns/state/<life_id>/_files/<relative-name>` | namespace-owned verbatim file | — (raw passthrough) | upper layers |
| `cas/manifests/<namespace>/<epoch-hex>-<seq-hex>/<ordinal>.zst` | part manifest | `cas_part_manifest` | part build |
| `gc/state` | GC state (incl. GC lease) | `cas_gc_state` | GC |
| `gc/hb` | GC leader heartbeat | `cas_gc_hb` | GC |
| `gc/maintenance_state` | leak-only namespace-janitor cursor | `cas_gc_maintenance_state` | future janitor |
| `gc/gen/<gen>/attempt/<att>/fold_seal` | fold seal (deterministic) | `cas_fold_seal` | GC |
| `gc/gen/<gen>/attempt/<att>/blob_target/<shard>/<seq>` | GC source-edge run segment | `cas_run` | GC |
| `gc/gen/<gen>/attempt/<att>/outcomes/<round>/<shard>.zst` | GC outcome log | `cas_gc_outcomes` | GC |
| `gc/server-roots/<server_root_id>/owner` | server-root owner singleton | `cas_owner` | mount |
| `gc/server-roots/<server_root_id>/epoch` | server-root epoch singleton | `cas_epoch` | mount |
| `gc/server-roots/<server_root_id>/mount` | mount lease (incl. `min_active` watermark) | `cas_mount_lease` | mount |
| `roots/<key>` | loose mountpoint object, verbatim | — (never interpreted) | upper layers |
| `staging/<server_root_id>/…` | S3-native upload staging scratch | — | writer, own mount only |

`<algo>` is `ch128`, `xxh3`, or `sha256` — the hash algorithm is a path segment because one pool may
legally hold blobs under several algorithms at once. `<hex[0:2]>` is a flat two-character S3 key
shard for request-fan-out, unrelated to the separate `gc_shards` GC-internal reduction fan-out
(which appears only inside `gc/gen/…` keys and routes by the digest's high 64 bits, read
big-endian). Discovery LISTs use fixed prefixes: `cas/ns/stream/`, `cas/ns/`, `cas/manifests/`,
`blobs/` (deliberately without the algorithm segment, so one recursive LIST covers every
algorithm), `roots/`, `gc/server-roots/`. `staging/` is a top-level sibling that no GC LIST ever
touches — it is reclaimed only by its own server's next mount.

## Envelope format {#envelope-format}

Every persisted CAS metadata object is text: a header line, a body, and an optional trailer.

```
{"type":"cas_<object>","v":N}          <- header line, always present
<body>                                  <- one JSON object, sorted NDJSON records,
                                            or a descriptor + raw payload zone
{"n":…}                                 <- optional trailer (record/entry count)
```

`v` is the only version field; a reader rejects `v` above what the build supports with
`UNKNOWN_FORMAT_VERSION`, checked before the body. A `.zst` key suffix means, exactly, that the
object kind's compression policy is `Always`: the object is stored as one zstd frame with the
checksum flag on, and its declared content size is checked against a per-kind cap before
allocation. Always-small and deterministic kinds (`cas_ref_ckpt`, `cas_blob_meta`, `cas_fold_seal`,
`cas_run`, …) are stored raw, with no `.zst` suffix.

The blob envelope is a special case of the header/body shape: a JSON descriptor padded with ASCII
spaces to a pool-constant `blob_header_len` (256 bytes, a `cas_pool_meta` field), terminated by
`\n`, so the raw payload always starts at that fixed offset with no header parse needed to locate
it. The part manifest is the other `PayloadHybrid` kind: text header, descriptor, sorted NDJSON
entry records, `{"n":…}` trailer, then a banner-framed raw payload zone for small inline file
bytes.

## Codec table {#codec-table}

Condensed from the authoritative traits table in `CasFormat.cpp` (`TRAITS`, asserted complete by
`gtest_cas_text_format.cpp`).

| Type string | Family | Key strictness | Compression |
|---|---|---|---|
| `cas_blob` | `PayloadHybrid` | tolerant | never (raw, fixed offset) |
| `cas_blob_meta` | `Control` | tolerant | never |
| `cas_pool_meta` | `Control` | tolerant | never |
| `cas_ref_log` | `Control` | tolerant | always (`.zst`) |
| `cas_ref_snap` | `Control` | tolerant | always (`.zst`) |
| `cas_ref_ckpt` | `Control` | strict | never |
| `cas_ref_catalog` | `Control` | strict | never |
| `cas_part_manifest` | `PayloadHybrid` | tolerant | always (`.zst`) |
| `cas_run` | `RecordStream` | strict | pinned raw |
| `cas_fold_seal` | `Control` | strict | pinned raw |
| `cas_gc_state` | `Control` | tolerant | never |
| `cas_gc_hb` | `Control` | tolerant | never |
| `cas_gc_outcomes` | `Control` | tolerant | always (`.zst`) |
| `cas_gc_maintenance_state` | `Control` | strict | never |
| `cas_owner` | `Control` | tolerant | never |
| `cas_epoch` | `Control` | tolerant | never |
| `cas_mount_lease` | `Control` | tolerant | never |

"Strict" means unknown keys are rejected rather than skipped, used for objects where every field
decides a durability or cleanup decision (`cas_ref_ckpt`, `cas_ref_catalog`, `cas_fold_seal`,
`cas_run`, `cas_gc_maintenance_state`); a `!`-prefixed key is always critical regardless of the
kind's strictness. "Pinned raw" objects (`cas_run`, `cas_fold_seal`) need stable bytes across
re-encodes for deterministic-artifact adoption, so their bytes are never recompressed once
written. `cas_blob` and `cas_part_manifest` are the `PayloadHybrid` family: a text descriptor
followed by a raw payload zone, rather than a single JSON body.

## Worked example tree {#worked-example}

Pool prefix `ca-pool`, server root `srv1`, one `Atomic` table, one part `all_1_1_0` with one blob
column file, written at `writer_epoch = 1, sequence = 3`:

```
ca-pool/_pool_meta

ca-pool/cas/ns/stream/0123456789abcdef0123456789abcdef/_log/0000000000000001-0000000000000003.zst
ca-pool/cas/ns/stream/0123456789abcdef0123456789abcdef/_snap/0000000000000001-0000000000000003.zst
ca-pool/cas/ns/state/0123456789abcdef0123456789abcdef/_ckpt

ca-pool/cas/manifests/srv1/store/3f2/3f2a1b7c-…-abcdefabcdef@cas@/0000000000000001-0000000000000003/000001.zst

ca-pool/blobs/xxh3/a1/a1b2c3d4e5f60708b1c2d3e4f5061728
ca-pool/blobs/xxh3/a1/a1b2c3d4e5f60708b1c2d3e4f5061728.meta

ca-pool/roots/srv1/clickhouse_access_check_8f3a1c2d

ca-pool/gc/state
ca-pool/gc/hb
ca-pool/gc/server-roots/srv1/{owner,epoch,mount}
ca-pool/gc/gen/7/attempt/1/fold_seal
ca-pool/gc/gen/7/attempt/1/blob_target/0/1
ca-pool/gc/gen/7/attempt/1/outcomes/1/0.zst

ca-pool/staging/srv1/<upload scratch>
```

`0123456789abcdef0123456789abcdef` is the opaque physical `life_id` the catalog maps the table's
namespace to; the ref log and snapshot keys reuse the same `RefTxnId` rendering
(`0000000000000001-0000000000000003`) as the manifest's build-scoped directory, but they are
different counters with different semantics, not the same identifier. The `data.bin` entry inside
the part manifest names the blob by `{XXH3_128, a1b2…1728}`, which is what resolves to the
`blobs/xxh3/a1/…` key above. A small file such as `count.txt` has no object of its own — it is
inline inside the manifest's raw payload zone, not a separate key.

## Notes {#notes}

- `cas/ns/state/<life_id>/_ckpt` carries **no** `.zst` suffix: `cas_ref_ckpt`'s compression policy
  is `never`, while its `_log`/`_snap` siblings in the same `cas/ns/` tree compress `always`.
- The namespace-stream tree is `cas/ns/stream/` (immutable `_log`/`_snap` objects) and
  `cas/ns/state/` (mutable `_ckpt`, verbatim `_files/`).
