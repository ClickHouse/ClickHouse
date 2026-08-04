---
description: 'System table containing the live mount and GC-health state of every server mounted onto a content-addressed (CAS) disk pool.'
sidebar_label: 'cas_mounts'
sidebar_position: 31
slug: /operations/system-tables/cas_mounts
title: 'system.cas_mounts'
doc_type: 'reference'
---

## Description {#description}

The `system.cas_mounts` table contains one row per mount slot discovered on every
content-addressed (CAS) disk configured on the node. A pool may be shared by several servers (or
several `server_root_id` mounts on the same server), and this table lists every mount visible in
the pool's backend at query time, not only the querying server's own mount — it exists for
incident-time diagnosis of leases, epochs, and GC leadership across a shared pool.

The table is read directly from the CAS disk's backend on every query (there is no persisted log
behind it); a transient backend error on one disk is skipped and does not blind the rest of the
rows.

## Columns {#columns}

- `disk` ([String](/sql-reference/data-types/string)) — Name of the content-addressed disk.
- `server_root_id` ([String](/sql-reference/data-types/string)) — Server root id owning the mount slot.
- `server_uuid` ([UUID](/sql-reference/data-types/uuid)) — UUID of the server incarnation holding the lease.
- `hostname` ([String](/sql-reference/data-types/string)) — Hostname recorded in the lease body.
- `process_id` ([UInt64](/sql-reference/data-types/int-uint)) — Process id recorded in the lease body.
- `writer_epoch` ([UInt64](/sql-reference/data-types/int-uint)) — Fenced writer epoch of the incarnation.
- `renewal_sequence` ([UInt64](/sql-reference/data-types/int-uint)) — Lease renewal sequence number.
- `started_at` ([DateTime64(3)](/sql-reference/data-types/datetime64)) — Time when the lease started.
- `expires_at` ([DateTime64(3)](/sql-reference/data-types/datetime64)) — Time when the lease expires.
- `min_active_build_sequence` ([UInt64](/sql-reference/data-types/int-uint)) — Oldest in-flight build sequence (`UINT64_MAX` means the mount said farewell).
- `gc_fenced` ([UInt8](/sql-reference/data-types/int-uint)) — `1` if GC fenced this slot out (terminal).
- `state` ([String](/sql-reference/data-types/string)) — One of `live`, `expired`, `terminated`, `fenced`, `corrupt`.
- `is_leader` ([Nullable(UInt8)](/sql-reference/data-types/nullable)) — `1` if this server's GC scheduler currently holds this disk's leadership lease.
- `pending_reclaim` ([Nullable(Int64)](/sql-reference/data-types/nullable)) — Cumulative two-phase deletion backlog observed by this process's GC on this disk (condemned entries minus executed exact-token deletes).
- `last_success_age_seconds` ([Nullable(UInt64)](/sql-reference/data-types/nullable)) — Seconds since this disk's GC last led a round (`0` if it has never led or GC is not running here).
- `wedged_namespace_count` ([Nullable(UInt64)](/sql-reference/data-types/nullable)) — Ref-append lanes currently wedged on this disk (an uncertain `PUT` exhausted its retry budget).
- `lifecycle` ([String](/sql-reference/data-types/string)) — This server's content-addressed pool lifecycle for the disk (a non-gated snapshot, always populated so a not-live disk stays visible): one of `live`, `not_live`, `identity_lost`, `vanished`, `constructing` (never started), or `shutdown` (torn down).
- `lifecycle_reason` ([String](/sql-reference/data-types/string)) — The enum-clean sub-state word for a `vanished` disk: `replaced` or `forgotten`. Empty for every other lifecycle, so `lifecycle || '(' || lifecycle_reason || ')'` reads e.g. `vanished(forgotten)`.
- `lifecycle_detail` ([String](/sql-reference/data-types/string)) — The full typed reason text naming the actual cause when not live: the vanish diagnosis (a data root replaced by a foreign pool, or decommissioned by `SYSTEM CAS FORGET` at a given time) or the identity-loss message. Empty when live.
- `lifecycle_since` ([Nullable(DateTime)](/sql-reference/data-types/nullable)) — When this server entered the current non-live lifecycle state. `NULL` when live, or when the state has no backing pool to date from.

`lifecycle`/`lifecycle_reason`/`lifecycle_detail`/`lifecycle_since` are the SQL surface for
diagnosing an identity-lost or forgotten disk without reading server logs — see the
`IdentityLost`/`VanishedReplaced`/`VanishedForgotten` states on the
[mount-slot behavioral model](/antalya/cas/architecture/mounts-and-leases#mount-state-machines) for
what each lifecycle value means, and [`SYSTEM CAS FORGET`](/antalya/cas/operations/debugging#sql-forget)
for the command that produces `vanished(forgotten)`.

:::note Local-only GC-health columns
`is_leader`, `pending_reclaim`, `last_success_age_seconds`, and `wedged_namespace_count` are process-local
facts about *this* server's own GC scheduler. They are populated **only** on the row whose `server_root_id` matches
this server's own mount, and are `NULL` on every row describing another server's mount — stamping a local
health fact onto a peer's row would misread as "the peer is the GC leader" during an incident. To see the
peer's own view of these columns, query `system.cas_mounts` on that server.
:::

## Example {#example}

```sql
SELECT
    disk,
    server_root_id,
    state,
    writer_epoch,
    is_leader,
    pending_reclaim,
    last_success_age_seconds
FROM system.cas_mounts
ORDER BY disk, server_root_id
FORMAT Vertical;
```

## See Also {#see-also}

- [`system.cas_gc_log`](/operations/system-tables/cas_gc_log) — per-round GC event log.
- [`system.cas_log`](/operations/system-tables/cas_log) — per-decision event log for the CAS garbage collector and writer.
- [`SYSTEM CAS GC RUN`](/sql-reference/statements/system#system-cas-gc-run) — run one GC round synchronously.
- [`SYSTEM CAS DROP POOL MEMBER`](/sql-reference/statements/system#system-cas-drop-pool-member) — permanently decommission a dead pool member's `server_root_id`.
