---
description: 'The three content-addressed system tables, a key-metrics table with healthy ranges, and queries for reading GC health from cas_gc_log.'
sidebar_label: 'Monitoring'
sidebar_position: 2
slug: /antalya/cas/operations/monitoring
title: 'CAS Operations — Monitoring'
doc_type: 'guide'
---

# Operations — monitoring {#monitoring}

Content-addressed (`CAS`) storage exposes three system tables and a family of `CAS`-prefixed
`ProfileEvents`. This page is the entry point for day-to-day health checks; see
[debugging](/antalya/cas/operations/debugging) for incident-time tooling and
[troubleshooting](/antalya/cas/operations/troubleshooting) for symptom-driven diagnosis.

## The three system tables {#system-tables}

| Table | Grain | Use it for |
|---|---|---|
| [`system.cas_mounts`](/operations/system-tables/cas_mounts) | One row per mount slot in the pool, read live from the backend on every query | Who is in the pool right now, lease/epoch state, which node holds GC leadership |
| [`system.cas_gc_log`](/operations/system-tables/cas_gc_log) | One `Start`/`Finish` row per GC round, plus one `Phase` row per phase reached | GC round outcomes, duration, and where a round's `LIST`/`GET`/`PUT`/`DELETE` budget went |
| [`system.cas_log`](/operations/system-tables/cas_log) | One row per writer/GC decision (blob puts, dedup adoptions, retire decisions, dangling-access findings) | Fine-grained forensics for one part, one blob hash, or one round |

`system.cas_mounts` is the only one of the three with no persisted backing log — it is a live view,
so a transient backend error on one disk is skipped rather than blinding the whole query. The other
two are ordinary `system.*_log` tables and follow the usual flush/retention settings.

## Key metrics {#key-metrics}

Every `CAS`-related `ProfileEvent` carries the uppercase `CAS`/`CASGC` prefix. This is a curated
subset for a first health pass; the full list groups by object class (`CASBlob*`, `CASManifest*`,
`CASRoot*`, `CASGC*`, `CASServer*`, `CASOther*`, `CASRef*`, `CASMeta*`) and is enumerated in
`src/Common/ProfileEvents.cpp`.

| Metric | Healthy range | A spike or nonzero means |
|---|---|---|
| `CASBlobCompareSwapConflict` | Near zero relative to `CASBlobCompareSwap` | Concurrent-update contention on blob metadata |
| `CASBlobHead` / `CASBlobHeadMiss` | Aggregate present/missing outcomes for every successful backend `HEAD` under `/blobs/`, across writer, `GC`, validation, and other callers | Global totals do not by themselves measure the one-`HEAD` writer budget or diagnose retries; attribute by query and path before drawing either conclusion |
| `CASBlobBodyPutAvoided` | Safe writer observations increment it when a physical body publication is avoided | Compare with query-attributed writer materialization tasks; the aggregate HEAD counters include unrelated callers |
| `CASRefAppendWedged` | Zero | A ref-log append lane exhausted its retries after an uncertain `PUT`; ref-log progress on that namespace may be stalled |
| `CASRefNeedsRecovery` | Zero | A ref-append lane could not install a known-durable transaction and now refuses writes, snapshots, and confirmation until durable replay completes |
| `CASRefAppendSealRejected` | Occasional (a deposed writer losing a race is the protocol working); sustained growth is not | A writer keeps retrying after losing its mount and does not yet know it |
| `CASGCHeartbeatFenceOuts` | Zero on a healthy pool | GC fenced an expired mount; check `system.cas_mounts` for a member that should have cleanly unmounted |
| `CASGCUnmatchedRemoveDeltas` | Occasional (benign per-key no-op by design) | A persistent nonzero rate means removal deltas are reaching the reducer without their matching activation — a correctness signal worth a look, not an automatic false deletion |
| `CASGCCondemnMarkerUnconfirmedCarry` | Zero | A durable condemn marker could not be confirmed; deletion is safely postponed but investigate marker write/read failures |
| `CASGCMetaWriteAnomaly` | Zero | The bounded GC metadata pool failed an operation; backend or pool pressure may delay metadata convergence |
| `CASRefRollbackBestEffortDropFailed` | Zero | A rollback cleanup drop hit a backend failure; refs may remain live and GC may be delayed on that namespace |

Two counter-reading caveats that apply to `system.events`-backed metrics generally, not only `CAS`
ones: a counter that has never incremented can be **absent** from `system.events` rather than
present at zero — query with `system_events_show_zero_values = 1` to tell "never happened" from "not
shown". A server restart resets `system.events` to zero, so a cumulative `CAS` total across a
restart has to be computed from summed per-second deltas in `system.metric_log`, not read directly
off `system.events`.

## Reading GC health from cas_gc_log {#gc-health}

Round outcomes over the last day, per disk:

```sql
SELECT disk_name, outcome, count() AS rounds, avg(duration_ms) AS avg_ms
FROM system.cas_gc_log
WHERE event_type = 'Finish' AND event_time > now() - INTERVAL 1 DAY
GROUP BY disk_name, outcome
ORDER BY disk_name, rounds DESC;
```

A steady stream of `Success` and `Deferred` rows is healthy; `Deferred` means the round found no
changed shard needing a fold and no graduation was due — a cheap round, not a stuck one (see
[the round](/antalya/cas/architecture/garbage-collection#the-round)). Recurring `Error` rows, or
`NotALeader` outcomes for the disk's own scheduler, warrant investigation. `anomalies` in the
`Finish` row is worth a steady watch: it is fold clamps surfaced and survived, so a non-zero value
that persists across rounds is more interesting than an isolated one.

Which phase dominates round duration or the `LIST` budget — reproduced from the
[per-phase rows](/operations/system-tables/cas_gc_log#per-phase-rows) reference:

```sql
SELECT phase,
       count() AS rounds,
       quantile(0.99)(phase_duration_microseconds) AS p99_microseconds,
       sum(ProfileEvents['S3ListObjects']) AS lists
FROM system.cas_gc_log
WHERE event_type = 'Phase' AND disk_name = 'cas'
GROUP BY phase
ORDER BY p99_microseconds DESC;
```

Pending-reclaim backlog and time since a disk's GC last led, from the live mount view:

```sql
SELECT disk, server_root_id, is_leader, pending_reclaim, last_success_age_seconds, wedged_namespace_count
FROM system.cas_mounts
WHERE is_leader IS NOT NULL
ORDER BY disk, server_root_id;
```

`is_leader`, `pending_reclaim`, `last_success_age_seconds`, and `wedged_namespace_count` are
process-local — `NULL` on every row describing a peer's mount — so this query is only informative
run against the node whose GC leadership you are checking; run it on each node to see the whole
pool's view of itself.
