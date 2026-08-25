---
description: 'Adding a content-addressed disk to an existing deployment, moving a partition onto it with ALTER TABLE MOVE PARTITION, rolling back, and permanently decommissioning a pool member.'
sidebar_label: 'Migration'
sidebar_position: 1
slug: /antalya/cas/operations/migration
title: 'CAS Operations — Migration'
doc_type: 'guide'
---

# Operations — migration {#migration}

This page walks through moving `MergeTree` data onto a content-addressed (`CAS`) disk from an
existing disk, and the reverse. `metadata_type = cas` is opt-in per disk (see the
[overview](/antalya/cas)), so this is an additive change to a running deployment: the existing
disk and its data are untouched until a partition is explicitly moved.

## Add a CAS disk alongside an existing one {#add-disk}

A storage policy can carry both an ordinary disk and a `CAS` disk as separate volumes. `ALTER TABLE
... MOVE PARTITION ... TO DISK` then moves data between them without an `INSERT`/`DROP` cycle. As on
the [configuration](/antalya/cas/configuration#disk-config) page, the recommended shape layers a
`type=cache` disk over the `CAS` disk, and the policy's volume references the **cached** disk name:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <local_disk>
                <type>local</type>
                <path>/var/lib/clickhouse/local_disk/</path>
            </local_disk>
            <cas>
                <type>object_storage</type>
                <object_storage_type>s3</object_storage_type>
                <metadata_type>cas</metadata_type>
                <cas_server_root_id>{replica}</cas_server_root_id>
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
            <tiered>
                <volumes>
                    <hot>
                        <disk>local_disk</disk>
                    </hot>
                    <cas_volume>
                        <disk>cas_cache</disk>
                    </cas_volume>
                </volumes>
            </tiered>
        </policies>
    </storage_configuration>
</clickhouse>
```

See [configuration](/antalya/cas/configuration) for the full disk-level settings surface and
[bucket requirements](/antalya/cas/bucket-requirements) for what the target bucket needs to
support. A table does not need to be created for the first time on `CAS` to use it — an existing
table just needs its storage policy widened to include a volume backed by a `CAS` disk, which is a
metadata-only change (`ALTER TABLE ... MODIFY SETTING storage_policy = ...`, subject to the usual
constraint that the new policy must still contain every volume and disk of the old one — a storage
policy can only grow, never lose a disk it once had).

## Move a partition onto CAS {#move-partition}

`ALTER TABLE ... MOVE PARTITION ... TO DISK` moves every part of one partition to the named disk in
place — the ordinary `MergeTree` partition-move mechanism, unchanged by `CAS`:

```sql
CREATE TABLE events (event_date Date, event_id UInt64, payload String)
ENGINE = MergeTree ORDER BY event_id PARTITION BY event_date
SETTINGS storage_policy = 'tiered';

INSERT INTO events VALUES ('2026-08-04', 1, 'hello'), ('2026-08-04', 2, 'world');

SELECT name, partition, disk_name FROM system.parts WHERE table = 'events' AND active;
```

```text
Row 1:
──────
name:      20260804_1_1_0
partition: 2026-08-04
disk_name: local_disk
```

The partition starts on `local_disk`, the first volume in the policy. Moving it onto `CAS` uploads
each part's files as content-addressed blobs, writes a part manifest, and publishes a ref — the same
write path an `INSERT` directly onto `CAS` takes (see
[what just happened](/antalya/cas/quick-start#what-happened) in the quick start). `TO DISK` names the
disk actually listed in the policy's volume — with a cache layered in front, that is the **cache**
disk's name (`cas_cache`), not the raw `CAS` disk's name (`cas`) underneath it; naming the raw disk
is refused, because it is not a member of the table's storage policy:

```sql
ALTER TABLE events MOVE PARTITION '2026-08-04' TO DISK 'cas_cache';

SELECT name, partition, disk_name FROM system.parts WHERE table = 'events' AND active;
```

```text
Row 1:
──────
name:      20260804_1_1_0
partition: 2026-08-04
disk_name: cas_cache
```

```sql
SELECT * FROM events ORDER BY event_id;
```

```text
2026-08-04	1	hello
2026-08-04	2	world
```

`system.parts.disk_name` reports the cache disk's name, not the underlying `CAS` disk's — this is
the ordinary `type=cache` disk behavior (the same happens layering a cache over any other disk type)
and is not `CAS`-specific. `system.filesystem_cache` shows the part's files populated into the
`cas_cache` cache on this read-through.

## Roll back {#rollback}

The move is symmetric: `MOVE PARTITION ... TO DISK` back onto the original disk name returns the
partition to its previous location, with the data intact throughout:

```sql
ALTER TABLE events MOVE PARTITION '2026-08-04' TO DISK 'local_disk';

SELECT name, partition, disk_name FROM system.parts WHERE table = 'events' AND active;
```

```text
Row 1:
──────
name:      20260804_1_1_0
partition: 2026-08-04
disk_name: local_disk
```

Moving a partition off `CAS` does not itself delete the blobs it stops referencing — dropping the
old ref makes them eligible for reclamation by the next
[GC round](/antalya/cas/architecture/garbage-collection), the same as dropping a part.

This exact three-disk, cache-over-`CAS` configuration and the forward/rollback `ALTER TABLE ... MOVE
PARTITION` sequence above were run against a live server before publication, using the `local`
object-storage backend for the `cas` disk: `CREATE TABLE`, `INSERT`, both `MOVE PARTITION`
directions, the `system.parts` checks, the `system.filesystem_cache` check, and the `SELECT` all
completed with zero errors and the shown output. A prior attempt to move onto `TO DISK 'cas'`
directly (the raw disk, not the cache) was refused with `All parts of partition '20260804' are
already on disk 'cas_cache'. (UNKNOWN_DISK)` — a real error message from the run, kept here because
it is exactly what an operator sees after guessing the wrong disk name.

## Permanently removing a pool member {#decommission}

A `CAS` pool can be shared by several servers (see [`server_root_id`](/antalya/cas/architecture/mounts-and-leases#server-root-id)).
Scaling down — permanently removing a server that will never rejoin the pool — is a distinct,
irreversible operation from an ordinary restart or a temporary outage: it fences the member's
`server_root_id` and reclaims the storage attributable only to it.

`SYSTEM CAS DROP POOL MEMBER` claims the victim's mount slot as an administrative writer (refusing
immediately if the member is still alive), drops every table namespace the member owned, sweeps
manifest debris, drains its staging and mountpoint objects, and — only once every drain is
confirmed — retires the mount slot itself. It emits ordinary ref-edge deltas rather than a GC
transition: it does not synchronously reclaim shared blob content, it only makes the now-unreferenced
blobs eligible for an ordinary GC round to reclaim later.

```sql
SYSTEM CAS DROP POOL MEMBER 'server_root_id' FROM DISK 'disk_name' [ON CLUSTER cluster_name]
```

Both `server_root_id` and `disk_name` are required string literals. The offline CLI twin,
`clickhouse-disks cas-drop-member <server_root_id>`, does the same work against a disk opened
read-only — the pool-admin claim happens internally, so the disk it runs against must not be the
live server's own mount:

```bash
clickhouse-disks -C config.xml --disk cas cas-drop-member 'replica-2'
```

The command returns one row (or, offline, one line per field) with `namespaces_removed`,
`namespaces_already_removed`, `committed_refs_removed`, `precommits_removed`,
`manifest_debris_removed`, `staging_objects_removed`, `mountpoint_objects_removed`, and
`slot_removed`. It is resumable: a rerun skips namespaces already marked removed and reports them
under `namespaces_already_removed` rather than redoing the work. A per-object drain failure is
recorded as a `warning` rather than raised as an exception, leaving the slot terminated but not
fully drained so a later invocation can resume; a non-empty `warnings` means exactly that, and the
mount slot stays in place as a resume anchor rather than being fully retired.

**Preconditions.** Confirm the member is actually and permanently dead before running this: the
operation fences that `server_root_id` out even if the server comes back online, and it deletes
namespace and drain state that cannot be recovered. Check `system.cas_mounts` for the member's
`state` and `last_success_age_seconds` first — a `live` row, or one with a recent lease renewal,
means the member is not a decommission candidate yet.

**Verification.** After the command reports `slot_removed = true` with no warnings, the member's
`server_root_id` no longer appears as a row in `system.cas_mounts` on any peer, and a subsequent
`SYSTEM CAS GC RUN` on the pool will no longer wait on or fence its heartbeat. See
[mount, unmount, crash](/antalya/cas/architecture/mounts-and-leases#mount-lifecycle) for how the
claim, drain, and retirement steps fit into the mount-slot lifecycle.
