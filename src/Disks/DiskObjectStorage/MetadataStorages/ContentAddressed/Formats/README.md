# CAS persisted formats — the living registry

Every persisted CAS object is a text file: header line `{"type":"cas_<object>","v":N}`, body
(one JSON object / sorted NDJSON records / raw payload zone), optional `{"n":…}` trailer.
Can-grow-large types are stored under a **`.zst` key suffix** and are ALWAYS one zstd frame
(checksum on; declared content size checked against the cap before allocation); always-small and
deterministic types are raw. `CasTextFormat.{h,cpp}` is the only code that knows this shape.

The object inventory is text end to end — there are no binary CAS formats and no protobuf
dependency. The GC source-edge data plane (`cas_run`) is sorted NDJSON written and read as a
stream (no seek); its integrity check is the whole-file seal checksum. The part manifest is the
one `PayloadHybrid` object: text header + descriptor meta + sorted entry records + `{"n":…}`
trailer, followed by a banner-framed raw payload zone for inline file bytes.

**Rule:** any change to a persisted format lands in the SAME commit as its row here.

## Bucket map

| Key (under the pool prefix) | Object | Codec | Writer |
|---|---|---|---|
| `_pool_meta` | pool identity + floors (`pool_id`, `blob_header_len`, `gc_shards`, `min_reader_generation`, `algos_used` array) | `CasPoolMetaFormat` | pool create/admit |
| `cas/ns/stream/<life_id>/_log/…​.zst` | ref transaction log (`namespace`, `txn_epoch`/`txn_seq`, optional critical `!prev_epoch`/`!prev_seq`; `set_published_at` uses `ref`/`published_ms`) | `CasRefLogFormat` (`.zst`) | writer commit path |
| `cas/ns/stream/<life_id>/_snap/…​.zst` | complete live ref table (`namespace`, `snapshot_epoch`/`snapshot_seq`, `lifecycle:"live"`; `kind` is `committed`/`precommit`, with `ref` and committed-only `published_ms`) | `CasRefSnapshotFormat` (`.zst`) | writer/GC fold |
| `cas/ns/state/<life_id>/_ckpt` | mutable life checkpoint (`life_epoch`, `committed_epoch`/`committed_seq`, `snapshot_epoch`/`snapshot_seq`, `seal_epoch`/`seal_seq`) | `CasRefCkptFormat` | writer/GC fold |
| `cas/ns/state/<life_id>/_files/…​` | namespace-owned raw files | — | upper layers |
| `cas/ref_catalog` | namespace lifecycle catalog (`kind:"entry"`, `ns`, `state`, `life`, `remove_round`, `creator`, `creator_epoch`, `creator_fence`) | `CasRefCatalogFormat` | namespace admission/removal |
| `cas/manifests/<ns>/<epoch-hex>-<seq-hex>/<ordinal>.zst` | part manifest (`namespace`, `payload_digest`; entry `path`, `place`, `size`) | `CasPartManifestFormat` | part build |
| blob keys (`CasLayout::blobKey`) | blob envelope (`type`, `v`, `tag`, `build`, `time_ms`, `creator`, `op`, `chver`, `ref`) + payload | `CasBlobEnvelopeFormat` | uploads |
| blob-meta keys (`CasLayout::blobMetaKey`) | freshness sidecar (`state`, `condemn_round`, `size`) | `CasBlobMetaFormat` | dedup/GC |
| `gc/state`, `gc/hb` | GC state (`round`, `gc_shards`, `snap_generation`, `snap_pruned_through`, `snap_attempt`, `manifest_sweep_cursor`, `lease_owner`, `lease_seq`) / heartbeat (`owner`, `hb_seq`) | `CasGcStateFormat` | GC |
| `gc/maintenance_state` | leak-only namespace-janitor cursor (`janitor_cursor`) | `CasGcMaintenanceStateFormat` | future janitor |
| `gc/gen/<g>/attempt/<a>/outcomes/…​.zst` | outcome log (`kind`, `outcome`) | `CasGcOutcomesFormat` (`.zst`) | GC |
| `gc/gen/<g>/attempt/<a>/fold_seal` | fold seal (deterministic; `generation`/`parent_generation`, `kind`: `ref_life`/`blob_run`/`condemned`) | `CasFoldSealFormat` | GC |
| `gc/gen/<g>/…​/runs` | GC source-edge record-stream runs (`ref`, `src`, `mark`; condemned: `pending`, `size`, `condemn_round`, `confirmed`) | `CasRecordStreamFormat` | GC |
| `gc/server-roots/<srid>/{owner,epoch,mount}` | server-root singletons (`server_uuid`, optional `retired_at_ms`; `next_writer_epoch`; `server_uuid`, `writer_epoch`, `hostname`, `pid`, `started_at_ms`, `seq`, `expires_at_ms`, `min_active_build_sequence`, `gc_fenced`, `write_attempt_id`) | `CasServerRootFormats` | mount |
| `roots/…` | raw passthrough (verbatim) | — (never interpreted) | upper layers |

## Codec table

Authoritative per-format traits (type string, family, strictness, compression policy, caps) live
in `CasFormat.cpp` (`TRAITS`), asserted complete by `gtest_cas_text_format.cpp`.

Key naming follows a deliberate split between metadata written once per object and fields repeated
once per record, not a flat character-count budget:

- metadata written once per object (`namespace`, `writer_epoch`, `blob_header_len`, …) uses
  descriptive names;
- fields repeated once per record (`ref`, `mark`, `op`, `class`, `place`, …) use short, semantic
  words whose meaning is clear in the record rather than the C++ member name verbatim;
- the fixed `cas_blob` descriptor uses its own separately budgeted compact vocabulary (`tag`,
  `build`, `chver`, …), because it must fit before the pool-wide fixed payload offset;
- common framing stays `type`, `v`, and `n`;
- `!` stays the must-understand prefix for critical fields;
- C++ member names obey an asymmetric rule: a member may be fuller than its wire key, never more
  cryptic than it.

Exact full C++ member names everywhere were deliberately rejected — see
`docs/superpowers/specs/2026-08-28-cas-semantic-wire-keys-design.md` ("Rejected alternatives").
Fixed-width `UInt128` identities render as 32-char lowercase hex strings; blob digests render as
algo-width hex (two chars per digest byte), with their algo name (`sha256:ab12…`) wherever a bare
hex would be ambiguous; unbounded u64 = decimal strings; bounded counts/lengths/ms-timestamps =
numbers; units documented here per object as codecs land.

`CasWireVocab.{h,cpp}` owns repeated value fields: `BlobRef` uses `algo`/`digest`, a persisted `Etag`
uses the jointly required `token_type`/`token` (the wire key spellings predate, and are independent
of, the C++ type's own name), `ManifestRef` uses `epoch`/`build`/`ord`, and owner-transition bindings
use the corresponding `old_*` and `new_*` key bundles.

## Evolution rules (one screen)

- `v` (header line) is the ONLY version field; reader gate: `v > G_BUILD` →
  `UNKNOWN_FORMAT_VERSION`, checked before the body.
- Additive change = new tolerant key, no `v` bump; on MUTABLE objects the field is best-effort
  until the pool floor rises (an old writer's fresh re-encode drops it).
- Breaking change = `v` bump + `changePoints` + write-down-to-floor; the floor raise is what
  fences old builds out (mount gates: `min_reader_generation` forward, pool-meta `v` backward).
- Deterministic formats (`cas_fold_seal`, `cas_run`): strict keys, pinned raw, and the adoption
  pin — on a `putDeterministicArtifact` conflict, re-encode at the `v` of the EXISTING object.
- A key prefixed `!` is critical: a reader that does not understand it fails closed.
- Padding zones (blob header pad, manifest banners) are deterministic and verified — no
  unaccounted bytes in any object.
- `openObject` policy asymmetry: a compressed body under a raw-compression policy is rejected
  (`CORRUPTED_DATA`), but a NON-zstd body under an `Always` policy is passed through verbatim as
  canonical text — an intentional uncompressed-repair path, not an "`Always` ⇒ must be compressed"
  enforcement. In practice the mismatch never arises: `Always` objects are read via a constructed
  `.zst`-suffixed key, so a raw body is not GETtable at that key.

## Generation history {#generation-history}

The format's generation history was reset to a flat `{1, 1}` baseline (`G_BUILD == 1`): CAS is
pre-release, carries no persisted data, and pays no compatibility cost for starting the count over.
Every class's `changePoints` begins at generation 1; a future breaking change appends a real entry to
that class's own array and bumps `G_BUILD`, the same way it always has.
