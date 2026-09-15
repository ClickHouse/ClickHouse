---
description: 'Every disk-level and server-level setting content-addressed storage exposes, generated from ContentAddressedSettings and ServerSettings at HEAD.'
sidebar_label: 'Configuration'
sidebar_position: 3
slug: /antalya/cas/configuration
title: 'CAS Configuration Reference'
doc_type: 'reference'
---

# Configuration reference {#configuration-reference}

## The disk config block {#disk-config}

A `CAS` disk is an `object_storage` disk with `metadata_type` set to `cas` and an explicit
`cas_server_root_id`. The recommended shape layers a `type=cache` disk in front of it — the local
filesystem cache absorbs repeated reads of the same blob, while the `CAS` disk underneath stays the
single source of truth the pool's other members and GC also read from. The storage policy references
the **cached** disk, not the raw `CAS` disk directly. `http_keep_alive_timeout` and
`http_keep_alive_max_requests` are set here for the reason explained under
[recommended keep-alive settings](#recommended-keep-alive-settings):

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <cas>
                <type>object_storage</type>
                <object_storage_type>s3</object_storage_type>
                <metadata_type>cas</metadata_type>
                <cas_server_root_id>{replica}</cas_server_root_id>
                <endpoint>https://bucket.s3.amazonaws.com/cas/</endpoint>
                <access_key_id>...</access_key_id>
                <secret_access_key>...</secret_access_key>
                <http_keep_alive_timeout>30</http_keep_alive_timeout>
                <http_keep_alive_max_requests>10000</http_keep_alive_max_requests>
            </cas>
            <cas_cache>
                <type>cache</type>
                <disk>cas</disk>
                <path>/var/lib/clickhouse/cas_cache/</path>
                <max_size>10Gi</max_size>
            </cas_cache>
        </disks>
        <policies>
            <cas>
                <volumes>
                    <main>
                        <disk>cas_cache</disk>
                    </main>
                </volumes>
            </cas>
        </policies>
    </storage_configuration>
</clickhouse>
```

`path` and `max_size` are ordinary `type=cache` disk settings (see
[external disk cache](/operations/storing-data#using-local-cache)), not `CAS`-specific — size the
cache to the working set of blobs a node reads repeatedly, not to the pool's total size. `type`,
`object_storage_type`, `metadata_type`, `endpoint`, `access_key_id`, `secret_access_key`, and the
other generic object-storage/disk keys (`path`, `name`, `region`, `use_environment_credentials`,
`readonly`, `use_fake_transaction`, and a handful more) belong to the shared disk layer, not to
`CAS` — they are accepted inside the `cas` disk's own block but are not `CAS` settings. `CAS`
validates its `cas_` namespace and leaves every other key, apart from the temporary unprefixed
aliases described below, to its relevant consumer.

The bare, uncached form — a storage policy pointing directly at the `CAS` disk, as used by
[quick start](/antalya/cas/quick-start) — remains valid and is the minimal way to try `CAS` out:

```xml
<policies>
    <cas>
        <volumes>
            <main>
                <disk>cas</disk>
            </main>
        </volumes>
    </cas>
</policies>
```

## Disk-level settings {#disk-settings}

The disk element is read by several components at once. `CAS` settings carry the `cas_` prefix;
every other key belongs to the object-storage or generic disk layer.

`CAS` is experimental: any setting below may change semantics, change its default, or disappear
entirely before release. Treat this table as a snapshot of the current build, not a stable contract.

| Setting | Default | Description |
|---|---|---|
| `cas_server_root_id` | — (required) | Explicit layout subtree identity; macros expand as in the `s3` `endpoint`. Anchored in the pool by a write-once owner claim — a colliding identity is refused at mount |
| `cas_scratch_path` | `<clickhouse-path>/disks/<disk_name>/cas_scratch/` | Server-local scratch dir for the write-buffer spill; a relative value is anchored to the server data path |
| `cas_gc_enabled` | `true` | Run the background GC scheduler on this disk. `false` is a debugging aid, not an operating mode: garbage then accumulates indefinitely and silently — watch `system.cas_gc_log` for round activity if you ever toggle it |
| `cas_gc_interval_sec` | `60` | Seconds between background GC rounds (≥ 1) |
| `cas_blob_hash` | `cityhash128` | Pool blob content-hash function (`cityhash128` \| `xxh3-128` \| `sha256`). Recorded in the pool at creation; a mismatching config is refused at mount |
| `cas_blob_hash_allow_new` | `false` | Explicit opt-in to admit a new hash algorithm into an existing pool. One-way: once admitted, the pool carries both algorithms permanently |
| `skip_access_check` | `false` | Skip the boot-time capability probe (start now, fix later). Only the preflight probe is skipped — the conditional-write correctness check still runs on every writable mount. **Not available on a writable generation-token (GCS) disk**, which refuses to mount with it: there, the probe battery is the only proof that a token-exact delete carries its generation precondition. Mount such a disk read-only if you need to defer the check |
| `cas_mount_lease_ttl_ms` | `30000` | Milliseconds for which a mount lease remains valid after a successful claim or renewal (≥ 1). Lower values shorten stale-mount recovery but reduce tolerance for object-storage and scheduling delays |
| `cas_mount_renew_period_ms` | `10000` | Milliseconds between background mount-lease renewals (≥ 1). It must leave enough time for two attempt envelopes (a renewal write and its settlement read) and the lease safety margin before the TTL expires: `period + 2 × envelope + margin < TTL` |
| `cas_gc_snapshot_generations_to_keep` | `3` | GC snapshot generations retained |
| `cas_gc_shards` | `1` | Blob-hash-prefix reducer shards (≥ 1). Recorded in the pool at creation; a mismatching config is refused at mount |
| `gcs_max_conditional_put_bytes` | 1 GiB | Largest conditional non-blob `PUT` on a generation-token store, including create-if-absent metadata/control artifacts and conditional replacements. Blob publication is unconditional, uses ordinary multipart, and is not subject to this cap |
| `cas_part_folder_cache_bytes` | 64 MiB | Part-folder view cache byte budget (`0` disables retention) |
| `cas_part_folder_cache_max_entries` | `10000` | Part-folder view cache entry cap |
| `cas_part_folder_cache_max_entry_bytes` | 16 MiB | Oversized part-folder views bypass retention above this size |
| `cas_manifest_decode_cache_bytes` | 128 MiB | Manifest decode cache byte budget (`0` disables) |
| `cas_gc_meta_pool_size` | `16` | Bounded pool size for GC per-hash freshness-meta writes |
| `cas_gc_read_concurrency` | `16` | Bounded pool size for the GC fold's read-ahead of checkpoints, ref logs, manifests and zero-candidate HEADs; `1` disables |
| `cas_attempt_timeout_ms` | `5000` | Budget for one HTTP attempt of a writable Native mount's control-plane requests (read, head, list, remove, conditional write), at least 1. Together with the connect cap it forms the attempt envelope (`cas_attempt_timeout_ms + 2 × cap`; the cap is `cas_attempt_timeout_ms` itself when the disk's `connect_timeout_ms` is `0`, else `min(connect_timeout_ms, cas_attempt_timeout_ms)`) that the lease arithmetic reserves: one TCP connect and one TLS handshake under the cap each, send/receive bounded per socket operation by `cas_attempt_timeout_ms`. With background renewal the cadence check requires `cas_mount_renew_period_ms + 2 × envelope + cas_lease_safety_margin_ms < cas_mount_lease_ttl_ms`, which puts an effective ceiling on the frozen connect cap: under the defaults (TTL 30000, period 10000, margin 2000) the envelope must stay under 9000, so a disk `connect_timeout_ms` of 2000 ms or more refuses to open writable — lower the connect timeout or raise the TTL if you hit this |
| `cas_lease_safety_margin_ms` | `2000` | Startup-only margin validated against the mount lease TTL: the attempt envelope + `cas_lease_safety_margin_ms` must be strictly less than the mount lease TTL, and `cas_mount_renew_period_ms` + 2 × envelope + `cas_lease_safety_margin_ms` too, or the disk refuses to open writable |
| `cas_unsafe_remount_no_delay` | `0` | Reclaim a mount slot that carries this server's own uuid at once after a hard restart, without observing the slot's token for the lease TTL. Unsafe whenever two processes can hold the same `server_uuid` (a copied uuid file, a stalled predecessor). After such a reclaim the predecessor can still start conditional writes until its own cutoff (`confirmed deadline − cas_lease_safety_margin_ms − 2 × envelope`) or until its next renewal meets the token guard, and a request it already sent may still materialize later. That is not a data hazard: ref-log keys carry `(writer_epoch, sequence)` and creates are conditional, so two writers can never commit different bodies to one key, and recovery's epoch seal settles any straggler (recovery fails closed after 64 successive seal-create attempts displaced by newly materializing old-epoch transactions). The exposure is availability, not data. Intended for test stands and deployments that guarantee one process per uuid |
| `cas_staging_backend` | `local` | Blob staging backend (`local` \| `s3`); `s3` is opt-in and requires native same-store copy on writable mount |

All servers sharing a pool must run the same `cas_mount_lease_ttl_ms` and `cas_mount_renew_period_ms`.
Startup reclaim and GC's fence-out both judge liveness by the mount slot's write token holding stable
on the observer's own `CLOCK_BOOTTIME`, using the observer's own threshold — nothing about a writer's
timing travels on the wire. Startup observes `cas_mount_lease_ttl_ms + floor(cas_mount_lease_ttl_ms /
20) + max(1, floor(cas_mount_renew_period_ms / 2))`; GC observes `cas_mount_lease_ttl_ms +
floor(cas_mount_lease_ttl_ms / 20) + cas_mount_renew_period_ms`. A pool member or GC leader
configured with a shorter threshold than its peers can therefore fence out a healthy peer whose
token-update gap exceeds that shorter threshold — a peer renewing frequently stays live, one that
missed a renewal does not. Change these values only with every member of the pool stopped: a
graceful restart removes only that member's own startup observation and does not make mixed
thresholds safe.

A shorter TTL reduces the tolerance for object-storage delays; a shorter renewal period increases it
(renewal starts earlier) at the cost of more background traffic. With the defaults,
`cas_mount_lease_ttl_ms − cas_lease_safety_margin_ms − cas_mount_renew_period_ms − 2 × envelope =
4000` ms is the scheduling-lateness budget before the first renewal attempt of a period can begin,
where `envelope = cas_attempt_timeout_ms + 2 × cap` (7000 ms with defaults) and `cap` is
`cas_attempt_timeout_ms` when the disk's `connect_timeout_ms` is `0`, else
`min(connect_timeout_ms, cas_attempt_timeout_ms)` (1000 ms with defaults); the renewal then keeps
retrying until `confirmed deadline − cas_lease_safety_margin_ms`.

The `expires_at_ms` stamped into the mount object is a writer-stamped diagnostic used by
`system.cas_mounts` and by the non-authoritative decommission epoch-recovery precheck; it never
authorizes a reclaim or a GC fence-out. Local fencing is derived instead from the confirmed
request's pre-I/O `CLOCK_BOOTTIME` anchor plus the TTL.

## Recommended keep-alive settings {#recommended-keep-alive-settings}

On a `CAS` disk, set `http_keep_alive_timeout` to `30` and `http_keep_alive_max_requests` to `10000`,
alongside the disk's other settings:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <ca>
                <type>object_storage</type>
                <object_storage_type>s3</object_storage_type>
                <metadata_type>cas</metadata_type>
                <cas_server_root_id>{replica}</cas_server_root_id>
                <endpoint>https://example-bucket.s3.amazonaws.com/cas/</endpoint>
                <access_key_id>...</access_key_id>
                <secret_access_key>...</secret_access_key>
                <http_keep_alive_timeout>30</http_keep_alive_timeout>
                <http_keep_alive_max_requests>10000</http_keep_alive_max_requests>
            </ca>
        </disks>
    </storage_configuration>
</clickhouse>
```

The generic S3 default, `http_keep_alive_max_requests = 100`, is the whole lifetime of a
connection under `CAS`'s control-plane request rate rather than a headroom margin: every ~100
requests, a connection is torn down and recreated, and its local port then cycles through
`TIME_WAIT`. Under sustained load this churn exhausts the ephemeral port range
(`EADDRNOTAVAIL`) and starves the mount-lease renewal request. Raising the two settings above
removes that churn, with no measured cost.

## Advanced GC pacing settings {#advanced-gc-pacing-settings}

These settings bound individual phases of a `GC` round. The first two accept any `UInt64` value;
for the remaining caps, `0` means unbounded.

| Setting | Default | Bounds | Description |
|---|---|---|---|
| `cas_manifest_sweep_list_budget_keys` | `1000` | `UInt64` | Orphan-manifest sweep `LIST` budget per round |
| `cas_manifest_sweep_delete_budget_keys` | `100` | `UInt64` | Orphan-manifest sweep `DELETE` budget per round |
| `cas_gc_bulk_delete_chunk_keys` | `1000` | `1`–`1000` | Keys per batch delete request in GC's write-once families (owner-removed manifest bodies, covered ref logs and snapshots) |
| `cas_gc_round_graduation_budget` | `5000` | `0` = unbounded | Blob-graduation (`condemned` → `delete_pending`) cohort cap per round |
| `cas_gc_round_redelete_budget` | `5000` | `0` = unbounded | Exact-token re-delete cohort cap for prior `delete_pending` rows per round |
| `cas_gc_round_sweep_namespace_budget` | `20` | `0` = unbounded | Distinct namespaces per orphan-manifest sweep page whose protection view may be built |
| `cas_gc_round_sweep_recovery_op_budget` | `5000` | `0` = unbounded | Committed-tail ref-log `GET`/decode operations the orphan-manifest recovery walk may spend per round |
| `cas_gc_round_ref_cleanup_budget` | `5000` | `0` = unbounded | Ref-object cleanup cap for covered log and snapshot deletes per round |
| `cas_gc_round_prefix_wholesale_budget` | `20000` | `0` = unbounded | Generation-prefix wholesale-delete object cap during pruning per round |
| `cas_gc_round_handoff_prefix_wholesale_budget` | `5000` | `0` = unbounded | Post-`CAS` hand-off generation-prefix reclaim cap per round, reserved separately so pruning cannot starve the one-shot hand-off |
| `cas_gc_round_outcome_entry_budget` | `5000` | `0` = unbounded | `GcOutcomes` entry cap across the re-delete/spared audit log per round |

## Migration from unprefixed keys {#migration-from-unprefixed-keys}

The unprefixed spelling of a `CAS` setting is accepted for now and reported at server startup. It
will stop being accepted; update configurations to the `cas_` names in the table above.

Two keys deliberately remain unprefixed: `skip_access_check`, shared with the generic disk layer,
and `gcs_max_conditional_put_bytes`, an S3 client setting. The server-level
`skip_access_check` flag skips the generic disk access check, while the `CAS` capability probe is
governed by the disk's own `skip_access_check` key.

### Choosing `cas_blob_hash` {#choosing-blob-hash}

`cas_blob_hash` is fixed at pool creation, so pick it deliberately. `cas_blob_hash_allow_new` is the
escape hatch — it admits a second algorithm into an existing pool's `algos_used` rather than
requiring a fresh pool.

| Algorithm | Pick it for | Trade-off |
|---|---|---|
| `sha256` | Maximum safety | No known collision classes; slightly slower than the other two |
| `xxh3-128` | Maximum speed | Fastest, 128-bit, no known collision classes |
| `cityhash128` (default) | ClickHouse-ecosystem compatibility, and a possible future hash-reuse mode that avoids recomputation | Fast, but has a known class of collisions that occurs far more often than an ideal hash function would predict |

## Server-level settings {#server-settings}

Source: `ServerSettings.cpp`. This setting is process-wide rather than scoped to one disk block.

| Setting | Default | Description |
|---|---|---|
| `cas_blob_upload_pool_size` | `16` | Size of the dedicated server-wide thread pool used to upload blobs in parallel when committing a `CAS` part. Zero is rejected: the pool must have at least one thread |

## `SYSTEM CAS` commands {#system-commands}

`SYSTEM CAS GC RUN`, `SYSTEM CAS GC STOP`, `SYSTEM CAS GC START`, `SYSTEM CAS GC REBUILD`,
`SYSTEM CAS FSCK`, `SYSTEM CAS FORGET`, and `SYSTEM CAS DROP POOL MEMBER '<server_root_id>' FROM
DISK '<disk>'` operate on a mounted `CAS` disk. Introspection lives in `system.cas_log`,
`system.cas_gc_log`, and `system.cas_mounts`.
