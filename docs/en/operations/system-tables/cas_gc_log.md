---
description: 'System table containing per-round records of the content-addressed (CAS) MergeTree garbage collector.'
sidebar_label: 'cas_gc_log'
sidebar_position: 30
slug: /operations/system-tables/cas_gc_log
title: 'system.cas_gc_log'
doc_type: 'reference'
---

## Description {#description}

The `system.cas_gc_log` table contains per-round records of the
content-addressed (CAS) MergeTree garbage collector. For every garbage-collection round it stores a
`Start` row and a `Finish` row (like `system.part_log` stores events per data part), with the counts
of objects marked and deleted, the round duration, the outcome, and a per-round `ProfileEvents`
delta.

Between them it also stores one `Phase` row per GC phase the round reached, each carrying that
phase's own duration, its `ProfileEvents` delta, and its phase-specific counts. All rows of one round
share a `round_id`. See [Per-phase rows](#per-phase-rows).

Rounds are emitted both by the background GC scheduler (`trigger = 'Scheduled'`) and by the
synchronous [`SYSTEM CAS GC RUN`](/sql-reference/statements/system#system-cas-gc-run)
command (`trigger = 'Manual'`).

The table is created only if the `cas_gc_log` server setting is
specified (it is enabled by default in the shipped `config.xml`).

## Columns {#columns}

- `hostname` ([LowCardinality(String)](/sql-reference/data-types/lowcardinality)) — Host name of the server executing the round.
- `event_date` ([Date](/sql-reference/data-types/date)) — Event date.
- `event_time` ([DateTime](/sql-reference/data-types/datetime)) — Event time.
- `event_time_microseconds` ([DateTime64(6)](/sql-reference/data-types/datetime64)) — Event time with microseconds precision.
- `event_type` ([Enum8](/sql-reference/data-types/enum)) — `Start` or `Finish` of a GC round, or one `Phase` of it.
- `disk_name` ([LowCardinality(String)](/sql-reference/data-types/lowcardinality)) — The content-addressed disk the round ran on.
- `server_root_id` ([LowCardinality(String)](/sql-reference/data-types/lowcardinality)) — Identifies the mount whose GC scheduler ran this round. Distinguishes concurrent mounters of the same shared pool; join on this column when correlating rounds against [`system.cas_mounts`](/operations/system-tables/cas_mounts).
- `gc_id` ([String](/sql-reference/data-types/string)) — The GC scheduler instance id (which mounter ran the round).
- `trigger` ([Enum8](/sql-reference/data-types/enum)) — `Scheduled` (background tick) or `Manual` (`SYSTEM` command).
- `round` ([UInt64](/sql-reference/data-types/int-uint)) — The GC round number (`0` on a `Start` row).
- `outcome` ([Enum8](/sql-reference/data-types/enum)) — `Unknown` (on a `Start` row), `Success` (led, folded, and completed), `NotALeader` (another replica holds the GC lease), `Deferred` (led but took the skip-unchanged fast path — no fold ran, because no changed shard reached the fold threshold and no graduation was due), or `Error` (the round threw).
- `candidates_marked` ([UInt64](/sql-reference/data-types/int-uint)) — Objects retired (marked) this round.
- `objects_deleted` ([UInt64](/sql-reference/data-types/int-uint)) — Objects physically deleted this round.
- `objects_absent` ([UInt64](/sql-reference/data-types/int-uint)) — Retire candidates found already absent.
- `objects_replaced` ([UInt64](/sql-reference/data-types/int-uint)) — `412`-saves (a resurrection won the race against the delete).
- `objects_spared` ([UInt64](/sql-reference/data-types/int-uint)) — Candidates spared because their in-degree was greater than zero at recheck.
- `manifests_deleted` ([UInt64](/sql-reference/data-types/int-uint)) — Owner-removed manifest bodies physically deleted this round, counted separately from blob deletes.
- `entries_condemned` ([UInt64](/sql-reference/data-types/int-uint)) — Retired entries newly condemned this round (retired-cursor pipeline stage 1).
- `entries_graduated` ([UInt64](/sql-reference/data-types/int-uint)) — Retired entries newly floor-passed and republished `delete_pending` this round (pipeline stage 2; deleted the next round).
- `entries_redeleted` ([UInt64](/sql-reference/data-types/int-uint)) — Pending exact-token blob deletes executed this round (pipeline stage 3).
- `fence_outs` ([UInt64](/sql-reference/data-types/int-uint)) — Expired mounts fenced out by this round's heartbeat floor.
- `anomalies` ([UInt64](/sql-reference/data-types/int-uint)) — Fold clamps surfaced (and survived) this round. A steady non-zero value warrants a look at the round log details.
- `duration_ms` ([UInt64](/sql-reference/data-types/int-uint)) — The round wall-clock duration (on a `Finish` row).
- `error` ([String](/sql-reference/data-types/string)) — The exception text when `outcome = 'Error'`.
- `ProfileEvents` ([Map(LowCardinality(String), UInt64)](/sql-reference/data-types/map)) — On a `Start`/`Finish` row, the per-round `ProfileEvents` delta (the `CAS*` counters and S3/disk events for this round). On a `Phase` row, **that phase's** delta, so `GROUP BY phase` over `ProfileEvents['S3ListObjects']` attributes the round's `LIST` budget to the phase that spent it.
- `round_id` ([String](/sql-reference/data-types/string)) — The correlator for every row of one round attempt: its `Start`, each of its `Phase` rows, and its `Finish`. Minted per attempt, so unlike `round` it exists even for a round that never committed and for a round that never led. Group by this column to reconstruct one round.
- `phase` ([LowCardinality(String)](/sql-reference/data-types/lowcardinality)) — The GC phase this row describes; empty on `Start`/`Finish`. See [Per-phase rows](#per-phase-rows) for the phase list.
- `phase_duration_microseconds` ([UInt64](/sql-reference/data-types/int-uint)) — The wall-clock duration of this phase, in microseconds (`Phase` rows only). Microseconds rather than milliseconds because several phases are routinely sub-millisecond and the point of the row is to see when they are not.
- `phase_metrics` ([Map(LowCardinality(String), UInt64)](/sql-reference/data-types/map)) — Phase-specific semantic counts (`Phase` rows only) that a phase computes for itself and no `ProfileEvents` counter can supply. The verb counts ride the `ProfileEvents` column of the same row.

## Per-phase rows {#per-phase-rows}

Besides the `Start` and `Finish` row of each round, the collector emits one `Phase` row per GC phase.
Every row of one round attempt — `Start`, each `Phase`, and `Finish` — shares a `round_id`. A round
that defers, or that never acquires the GC lease, emits only the phases it actually reached; a round
that throws still emits the row of the phase it died in.

The phases, in execution order:

| `phase` | What it covers | Dominant I/O |
|---|---|---|
| `lease` | Acquire, renew, or observe the GC lease. The only phase a `NotALeader` round emits. | `gc/state` `GET` + compare-and-swap |
| `pre_fold_ref_drain` | Resolve catalog rows whose terminal fold evidence is already adopted before this invocation publishes or defers. | catalog `GET` + exact compare-and-swap |
| `heartbeat_floor` | Classify every mount slot and fence out the dead ones. | `LIST` of the mount prefix, one `GET` per mount, one `PUT` per fence |
| `defer_decision` | The skip-unchanged decision: graduation check plus the round's one enumeration of the ref prefix. | one full ref-prefix `LIST`, two fold-seal `GET`s |
| `parent_seal_read` | Capture the pre-fold seal's run refs for the hand-off reclaim. | one fold-seal `GET` |
| `fold_ref_group` | Regroup the round's enumeration into per-table listings — what this round will fold. | none |
| `fold_seal_read` | The adopted fold seal, read twice at the same generation and attempt. | two fold-seal `GET`s |
| `fold_ref_intake` | Read and fold every new ref log and the manifest bodies its edges name. | one `GET` per new log, one `GET` per manifest edge |
| `fold_reduce` | The per-shard in-degree merge: condemn, spare, graduate. | prior-run streaming `GET`s, one `HEAD` per zero-transition candidate, run `PUT`s |
| `fold_seal_write` | Publish the new fold seal. | one `PUT` |
| `pending_deletes` | The single content-delete site: exact-token deletes of previously published `delete_pending` entries, plus the outcome logs. | one `DELETE` per entry, one outcome-log `PUT` per shard |
| `meta_pool_wait` | Drain the round's per-hash freshness-meta writes. | none on this thread — see the caveat below |
| `round_commit` | The generation-retention prune and the round's single `gc/state` compare-and-swap. | prune `LIST`s and deletes, one compare-and-swap |
| `handoff_reclaim` | Wholesale-reclaim generations a moved run ref stranded below the retention cursor. | prefix `LIST`s and deletes |
| `manifest_deletes` | Exact-token deletes of owner-removed manifest bodies, after their decrements were adopted. | one `DELETE` per body |
| `namespace_cleanup` | Run one bounded `cas/ns/` page across the stream and state subtrees for the perpetual dead-life janitor. This phase is physical reclamation, not a lifecycle gate. | one namespace-root page `LIST`, catalog cut, exact-token deletes |
| `ref_object_cleanup` | Delete ref logs covered by both the durable fold cursor and a durable snapshot, plus superseded snapshots. | one `HEAD` + one `DELETE` per deletable object |
| `orphan_sweep` | The budgeted, cursor-paced orphan part-manifest backstop. | budgeted `LIST` and deletes |

Which phase dominates a round:

```sql
SELECT phase,
       count() AS rounds,
       quantile(0.5)(phase_duration_microseconds) AS p50_microseconds,
       quantile(0.99)(phase_duration_microseconds) AS p99_microseconds,
       sum(phase_duration_microseconds) AS total_microseconds
FROM system.cas_gc_log
WHERE event_type = 'Phase' AND disk_name = 'ca'
GROUP BY phase
ORDER BY total_microseconds DESC;
```

Which phase spends the `LIST` budget:

```sql
SELECT phase, sum(ProfileEvents['S3ListObjects']) AS lists
FROM system.cas_gc_log
WHERE event_type = 'Phase' AND disk_name = 'ca'
GROUP BY phase
ORDER BY lists DESC;
```

One round, in order — including a round that failed, which is why the correlator is `round_id` and
not `round`:

```sql
SELECT phase, phase_duration_microseconds, phase_metrics, ProfileEvents['S3ListObjects'] AS lists
FROM system.cas_gc_log
WHERE round_id = '...' AND event_type = 'Phase'
ORDER BY event_time_microseconds;
```

Two caveats when reading these rows:

- Work scheduled onto the GC meta pool runs on other threads, so the `meta_pool_wait` row's
  `ProfileEvents` delta is **empty by construction**. Read its `phase_metrics` `jobs_scheduled` /
  `jobs_completed` next to its duration instead: they distinguish a deep queue from a slow endpoint.
- Phase durations do not sum to the round's `duration_ms`. The round also performs untimed
  bookkeeping between phases, and the `Finish` row's `duration_ms` remains the authority on total
  round time.

## Example {#example}

```sql
SELECT
    event_type,
    disk_name,
    trigger,
    outcome,
    candidates_marked,
    objects_deleted,
    duration_ms
FROM system.cas_gc_log
ORDER BY event_time_microseconds DESC
LIMIT 2
FORMAT Vertical;
```

## See Also {#see-also}

- [`SYSTEM CAS GC RUN`](/sql-reference/statements/system#system-cas-gc-run) — run one GC round synchronously.
- [`system.cas_mounts`](/operations/system-tables/cas_mounts) — live per-`server_root_id` mount and GC-health state.
- [`system.cas_log`](/operations/system-tables/cas_log) — per-decision event log for the CAS garbage collector and writer.
- [`system.part_log`](/operations/system-tables/part_log) — the analogous per-part event log.
