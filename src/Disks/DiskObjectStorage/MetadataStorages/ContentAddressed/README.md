# Content-Addressed (CAS) metadata storage

This directory implements the `cas` metadata storage for
`DiskObjectStorage`: a "git for `MergeTree`" content-addressed pool over
S3-family object storage. Instead of per-part metadata files pointing at
randomly-named remote objects, every unique payload is stored **once** in a
shared pool under a key derived from its **content hash**, a part is an
immutable **manifest** listing its files, and a per-table **ref table** maps
part names to manifests. Replicas that share one pool deduplicate bytes
structurally: fetching a part can *relink* the sender's manifest entries by
hash instead of copying data. This replaces zero-copy replication's shared
mutable state with immutable, hash-addressed objects plus a small CAS
(compare-and-swap) protocol.

## The data model in five objects

- **Blob** — the unit of payload. Keyed by its content digest
  (`CasLayout::blobKey`); an envelope wraps the payload
  (`Formats/CasBlobEnvelopeFormat`). Blobs are logically immutable and shared: equivalent
  physical incarnations may replace one another under the same hash key. The pool
  is the only index, and identity is always established by (re-)hashing the
  bytes — there are no trust-the-checksum shortcuts. A freshness sidecar
  (`blobMetaKey` = `blobKey` + `.meta`) carries GC state per hash.
- **Manifest** — an immutable description of one part: the full file tree with
  per-file blob references or inline bytes. Small per-part files
  (`uuid.txt`, `checksums.txt`, ...) live *inside* the manifest as inline tree
  entries, not as separate objects. Keyed under
  `cas/manifests/<ns>/<epoch-hex>-<seq-hex>/<ordinal>.zst`.
- **Ref table** — the mutable naming layer, one namespace per table
  (`SERVER_ID/TABLE_UUID`), keyed under one LIFE of that namespace: an
  append-only transaction log plus periodic snapshots (`cas/ns/stream/<life_id>/...`), with mutable
  checkpoints and namespace files under `cas/ns/state/<life_id>/...`. The catalog resolves each opaque
  physical ID to its logical namespace life (see `NamespaceLifeId`); stream objects are replayed
  into an in-memory table mapping
  ref names (part directory names) to manifests. All mutations go through a
  precommit/promote two-step so a manifest always has an owner while visible.
- **Server root** — one per server (`server_root_id` in the disk config): a
  single-writer mount slot with lease + writer-epoch fencing
  (`gc/server-roots/<srid>/{owner,epoch,mount}`). A fenced (expired or stolen)
  writer can never mutate the pool again with stale state.
- **GC records** — round-based garbage collection state under `gc/...`:
  leader lease/state, per-generation source-edge runs (which manifests
  reference which blobs), fold seals, and outcome logs. GC computes blob
  in-degrees from the edges, *condemns* zero-in-degree blobs, and later
  deletes them with **exact-token** conditional deletes, so a concurrent
  writer publishing the same content under a fresh envelope always wins (revival is
  publication from the writer's own source — never a read of a condemned object).

Everything persisted is a text format with a versioned header line; see
`Formats/README.md` for the registry, key map, and evolution rules.

## Lifecycle walkthroughs

- **Write**: `ContentAddressedTransaction` buffers or spills file writes
  (`scratch_path`, or opt-in S3 staging), hashes the payload, and hands it to
  `Pool/CasPartWriteTxn`: start every materialization with blob `HEAD`, adopt a present
  non-condemned body or publish the writer's source unconditionally, reconcile `.meta` to
  `Clean`, and record explicit `Materialized` proof; then stage the manifest, `precommitAdd` the ref,
  then promote it to committed in the ref log.
- **Read**: disk path → `Parts/PartPathParser` (namespace + part + file) →
  ref table resolves the manifest → `Pool/CasManifestReader` locates the
  entry → blob `StoredObject`s or in-manifest inline bytes.
- **Fetch between replicas** (`ContentAddressedExchange`): the sender ships
  its manifest bytes; the receiver rebuilds the part by *adopting* the listed
  blobs by hash in its own namespace (no payload transfer) and publishes a
  fresh manifest. Any decode or promote failure just falls back to the
  ordinary byte fetch.
- **GC round** (`Gc/CasGc`, paced by `Gc/CasGcScheduler`): take/renew the
  leader lease, collect source edges, fold per-shard in-degrees, condemn new
  zero-in-degree blobs, exact-token-delete blobs condemned earlier whose
  state did not change, and seal the round with one CAS on `gc/state`.

## Source layout

The tree is **layered**: entry points at the top level, implementation in
per-subsystem subdirectories with a strict one-direction include rule.

```
Primitives → Formats → Backend → Pool → Gc → Tools ≈ Parts → facade (top level)
```

- **`Primitives/`** — the vocabulary, zero outward dependencies: `CasBlobDigest`
  (`BlobHashAlgo` + `BlobDigest` + `DigestCodec` + `BlobRef` — blob identity),
  `CasTypes.h` (the other identity types: `RootNamespace`, `Token`,
  `ManifestId`, `RefTxnId`), `CasNamespaceLifeId` (`NamespaceLifeId` — one LIFE of
  a namespace's ref layer, the pair every ref key is built from),
  `CasBlobHashingWriteBuffer` (streaming
  hash-and-passthrough machinery), `CasXxh3Streamer` (the isolated vendored
  xxHash wrapper), `CasCodecUtil` (identifier/hex codec helpers), `CasEvent`
  (audit-event POD + sink).
- **`Formats/`** — everything persisted: bytes **and** keys. The per-object
  text/format files (`CasFormat`, `CasTextFormat`, `CasPartManifestFormat`,
  `CasRefLogFormat`, …) plus `CasLayout` (the object-key schema). See
  `Formats/README.md` for the format registry.
- **`Backend/`** — the token-aware storage seam: `CasBackend` (the contract:
  `get`/`putIfAbsent`/`casPut`/`deleteExact` with CAS tokens, plus unconditional
  transport-only `publishBlob`),
  `CasObjectStorageBackend`, `CasInMemoryBackend`, `CasInstrumentedBackend`,
  `CasRequestControl` (single-attempt conditional non-blob writes, including create-if-absent
  artifacts and conditional replacements, with explicit
  state-aware retries), `CasProbe` (mount-time capability probe).
- **`Pool/`** — the pool engine: `CasPool` (composition root), `CasPartWriteTxn`
  (one-part write transaction), `CasRefLedger` + `CasRefProtocol` (ref-table
  log/snapshot/replay + intake), `CasServerRoot` (mount-claim protocol +
  single-writer slot + staging sweeper), `CasPoolMeta`, `CasBlobMeta`,
  `CasManifestReader`, `CasPlainObjects` (the `roots/...` verbatim
  passthrough), `CasMountRuntime` (fence state shared with lanes).
- **`Gc/`** — garbage collection: `CasGcScheduler` (pacing thread), `CasGc`
  (the round engine), `CasGcShardPlan` (sharding math), `CasBlobInDegree`,
  `CasOrphanManifestSweep`.
- **`Tools/`** — operator verbs (`clickhouse-disks`): `CasFsck`,
  `CasDecommission`, `CasInspect`.
- **`Parts/`** — part semantics over the pool: `PartPathParser` (the
  ClickHouse-path classifier), `PartFolderAccess` (`PartRefKey` + `Freshness`
  + `PartFolderValidate` + `PartFolderView` + `CachedPartFolderAccess`).
- **Top level (facade)** — the entry points: `ContentAddressedMetadataStorage`
  (the `IMetadataStorage` facade), `ContentAddressedTransaction` (the
  `IMetadataTransaction`, including the write buffers), `ContentAddressedExchange`
  (the replication seam).

## Include-direction rule

A file may include only its **own layer** and layers to its **left** in the
order above. `Tools` and `Parts` are siblings with no edges between them. This
is enforced by convention (README rule) — there is no CI check.

**Named exceptions** (deliberate):

- The staging sweeper in `Pool/CasServerRoot` bypasses `Backend` and reaches straight into
  `IObjectStorage`.
- `Backend` may read `Formats` traits (the provider-metadata mirror).

## Configuration

A CAS disk is an `object_storage` disk with `metadata_type` =
`cas`. Minimal example (see
`tests/config/config.d/cas_storage_policy_for_merge_tree_by_default.xml`
and its `_s3_` sibling for the lane configs used in CI):

```xml
<disks>
    <cas>
        <type>object_storage</type>
        <object_storage_type>s3</object_storage_type> <!-- or local -->
        <metadata_type>cas</metadata_type>
        <!-- Required, validated identity of the layout subtree this server owns.
             Must be unique per server sharing the pool. -->
        <server_root_id>replica-1</server_root_id>
        <path>cas_pool/</path>
        <!-- Real server-local scratch dir for the write-buffer spill. -->
        <scratch_path>cas_scratch/</scratch_path>
        <gc_enabled>1</gc_enabled>
        <gc_interval_sec>60</gc_interval_sec>
    </cas>
</disks>
```

`<readonly>1</readonly>` opens the disk in observe-only mode: no mount-slot
claim, no capability probe, no writes — the mode `clickhouse-disks` tools and
post-mortem inspection use. The full knob set (staging backend, cache sizes,
GC sharding, hash algorithm, request budgets) is parsed in
`ContentAddressedSettings.cpp`; each knob is documented at its declaration site. Blob publication
has no presence-cache setting: `HEAD` is mandatory. `gcs_max_conditional_put_bytes` applies to all
conditional non-blob writes, including create-if-absent artifacts and conditional replacements, but
not to multipart-capable blob publication.

## Operations and observability

- `clickhouse-disks` verbs (all require the disk opened read-only): `fsck`
  (independent reachability audit of refs → manifests → blobs), `cas-inspect`
  (decode one pool object by its raw key to JSON), `cas-gc-dryrun` (preview the
  next GC round's deletes), `cas-gc-rebuild` (disaster-recovery rebuild of the
  `gc/state` baseline), `cas-drop-member` (decommission a dead pool member).
- `system.cas_log` — one row per CAS protocol event
  (uploads, adopts, promotes, condemns, deletes, mount-slot writes, ...);
  the primary audit trail when investigating pool state.
- The GC and writer paths also emit `ProfileEvents` counters (grep
  `ProfileEvents.cpp` for `Cas`).

## Testing

- **Unit tests** (`unit_tests_dbms`): every CAS suite name starts with `Cas`, so
  `--gtest_filter='Cas*'` runs the whole set — including parameterized suites,
  whose instantiation prefixes are `Cas`-prefixed too so the `<Inst>/<Suite>`
  spelling still matches. `utils/cas-gate/generate_cas_suites.sh` fails loud on a
  CAS suite that does not match, so a new suite cannot silently sit outside the
  filter; `utils/cas-gate/run_cas_gate_per_suite.sh` runs them one process per
  suite, so an abort cannot hide the suites after it.
- **Stateless lanes**: the functional-test jobs "`cas storage`"
  (local object storage) and "`cas s3 storage`" run the whole
  stateless suite with `MergeTree` defaulting to a CAS disk. Tests that
  legitimately cannot run there carry the `no-cas-storage` tag.
- **Soak / chaos**: `utils/ca-soak/` — multi-replica docker-compose
  harnesses (fault proxies, GC sharding variants, AWS S3/GCS backends) and
  adversarial scenarios.

## Reading order

To understand a request end to end, read in this order:

1. `ContentAddressedMetadataStorage` — the facade / routing.
2. `Parts/PartFolderAccess` (`PartRefKey` → the folder view / cache).
3. `Pool/CasPool` — the pool composition root and `open` protocol.
4. `Pool/CasPartWriteTxn` — one-part write transaction.
5. `Gc/CasGc` — the GC round engine.
