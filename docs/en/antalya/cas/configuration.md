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
`server_root_id`. The recommended shape layers a `type=cache` disk in front of it — the local
filesystem cache absorbs repeated reads of the same blob, while the `CAS` disk underneath stays the
single source of truth the pool's other members and GC also read from. The storage policy references
the **cached** disk, not the raw `CAS` disk directly:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <cas>
                <type>object_storage</type>
                <object_storage_type>s3</object_storage_type>
                <metadata_type>cas</metadata_type>
                <server_root_id>{replica}</server_root_id>
                <endpoint>https://bucket.s3.amazonaws.com/cas/</endpoint>
                <access_key_id>...</access_key_id>
                <secret_access_key>...</secret_access_key>
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
`CAS` — they are accepted inside the `cas` disk's own block but are not `CAS` settings. Every key
below this line, and every key not in that shared set, is rejected as unknown.

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

None of these keys carry a `cas_`/`ca_` prefix — the disk block already scopes them.

`CAS` is experimental: any setting below may change semantics, change its default, or disappear
entirely before release. Treat this table as a snapshot of the current build, not a stable contract.

| Setting | Default | Description |
|---|---|---|
| `server_root_id` | — (required) | Explicit layout subtree identity; macros expand as in the `s3` `endpoint`. Anchored in the pool by a write-once owner claim — a colliding identity is refused at mount |
| `scratch_path` | server data path | Server-local scratch dir for the write-buffer spill; a relative value is anchored to the server data path |
| `gc_enabled` | `true` | Run the background GC scheduler on this disk. `false` is a debugging aid, not an operating mode: garbage then accumulates indefinitely and silently — watch `system.cas_gc_log` for round activity if you ever toggle it |
| `gc_interval_sec` | `60` | Seconds between background GC rounds (≥ 1) |
| `blob_hash` | `cityhash128` | Pool blob content-hash function (`cityhash128` \| `xxh3-128` \| `sha256`). Recorded in the pool at creation; a mismatching config is refused at mount |
| `blob_hash_allow_new` | `false` | Explicit opt-in to admit a new hash algorithm into an existing pool. One-way: once admitted, the pool carries both algorithms permanently |
| `skip_access_check` | `false` | Skip the boot-time capability probe (start now, fix later). Only the preflight probe is skipped — the conditional-write correctness check still runs on every writable mount. **Not available on a writable generation-token (GCS) disk**, which refuses to mount with it: there, the probe battery is the only proof that a token-exact delete carries its generation precondition. Mount such a disk read-only if you need to defer the check |
| `deduplication_cache_bytes` | 64 MiB | Byte budget of the blob presence cache (`0` disables) |
| `deduplication_head_first_min_bytes` | 1 MiB | Minimum blob size to try a `HEAD` before uploading the body |
| `gc_snapshot_generations_to_keep` | `3` | GC snapshot generations retained |
| `gc_shards` | `1` | Blob-hash-prefix reducer shards (≥ 1). Recorded in the pool at creation; a mismatching config is refused at mount |
| `gcs_max_token_producing_put_bytes` | 1 GiB | Largest token-producing write on a generation-token store, conditional or not (GCS forces those single-part) |
| `part_folder_cache_bytes` | 64 MiB | Part-folder view cache byte budget (`0` disables retention) |
| `part_folder_cache_max_entries` | `10000` | Part-folder view cache entry cap |
| `part_folder_cache_max_entry_bytes` | 16 MiB | Oversized part-folder views bypass retention above this size |
| `part_folder_validate` | `always` | Cache body re-proof policy (`always` \| `never` \| `age <seconds>`). **Leave at `always`**: the other modes trade the fail-closed body-existence check for an optimization — this is a trust decision about unverified data, not a performance knob |
| `manifest_decode_cache_bytes` | 128 MiB | Manifest decode cache byte budget (`0` disables) |
| `gc_meta_pool_size` | `16` | Bounded pool size for GC per-hash freshness-meta writes |
| `staging_backend` | `local` | Blob staging backend (`local` \| `s3`); `s3` is opt-in |

### Choosing `blob_hash` {#choosing-blob-hash}

`blob_hash` is fixed at pool creation, so pick it deliberately. `blob_hash_allow_new` is the
escape hatch — it admits a second algorithm into an existing pool's `algos_used` rather than
requiring a fresh pool.

| Algorithm | Pick it for | Trade-off |
|---|---|---|
| `sha256` | Maximum safety | No known collision classes; slightly slower than the other two |
| `xxh3-128` | Maximum speed | Fastest, 128-bit, no known collision classes |
| `cityhash128` (default) | ClickHouse-ecosystem compatibility, and a possible future hash-reuse mode that avoids recomputation | Fast, but has a known class of collisions that occurs far more often than an ideal hash function would predict |

## Server-level settings {#server-settings}

Source: `ServerSettings.cpp`. Unlike the disk-level list, these carry the `cas_` prefix because they
are process-wide, not scoped to one disk block.

| Setting | Default | Description |
|---|---|---|
| `cas_blob_upload_pool_size` | `16` | Size of the dedicated server-wide thread pool used to upload blobs in parallel when committing a `CAS` part. Zero is rejected: the pool must have at least one thread |

## `SYSTEM CAS` commands {#system-commands}

`SYSTEM CAS GC RUN`, `SYSTEM CAS GC STOP`, `SYSTEM CAS GC START`, `SYSTEM CAS GC REBUILD`,
`SYSTEM CAS FSCK`, `SYSTEM CAS FORGET`, and `SYSTEM CAS DROP POOL MEMBER '<server_root_id>' FROM
DISK '<disk>'` operate on a mounted `CAS` disk. Introspection lives in `system.cas_log`,
`system.cas_gc_log`, and `system.cas_mounts`.
