---
description: 'SQL-first CAS debugging: live investigation queries against cas_log/cas_gc_log/cas_mounts/blob_storage_log, SYSTEM CAS FSCK/GC RUN/GC STOP-START/FORGET, and the offline clickhouse-disks tools for when the server cannot answer.'
sidebar_label: 'Debugging'
sidebar_position: 4
slug: /antalya/cas/operations/debugging
title: 'CAS Operations — Debugging'
doc_type: 'guide'
---

# Operations — debugging {#debugging}

Debugging a content-addressed (`CAS`) incident starts on a **live server**, with SQL: the three
system tables plus `SYSTEM CAS` commands cover reachability checks, forced GC rounds, and
per-object/per-round forensics without ever touching the bucket directly. The offline
`clickhouse-disks` tools at the [end of this page](#offline-tools) are the fallback for when SQL
cannot reach the pool at all — the server is down, or the access is deliberately read-only forensic.

## Investigating on a live server {#live-investigation}

See [monitoring](/antalya/cas/operations/monitoring#system-tables) for the three system tables'
grain and general health queries; this section is investigation queries for a specific incident,
not a health dashboard.

### What happened to this part or blob {#part-blob-history}

`system.cas_log` carries one row per writer/GC decision, keyed by `ref_name` (a part name) or
`object_hash` (a blob's content hash):

```sql
SELECT event_time_microseconds, event_type, outcome, reason, object_kind, object_hash, token, round, detail
FROM system.cas_log
WHERE disk_name = 'cas' AND ref_name = '<part_name>'
ORDER BY event_time_microseconds;
```

```sql
SELECT event_time_microseconds, event_type, outcome, reason, ref_name, round, detail
FROM system.cas_log
WHERE disk_name = 'cas' AND object_kind = 'blob' AND object_hash = '<hex_digest>'
ORDER BY event_time_microseconds;
```

`outcome` (`ok`, `adopt`, `resurrect`, `deleted`, `replaced`, `spared`, `absent`, `zeroed`,
`skipped`) and `reason` are the two columns to read first; `detail` is a
`Map(LowCardinality(String), String)` of decision-specific facts (`condemn_round`,
`superseded_token`, `code`, `site`) worth `arrayJoin(detail)` when the summary columns alone do not
explain the decision. See [`system.cas_log`](/operations/system-tables/cas_log) for the full column
reference.

### Why GC is not reclaiming {#why-not-reclaiming}

Two questions, in order: is this node's scheduler leading, and did its recent rounds actually fold?

```sql
SELECT server_root_id, is_leader, state, last_success_age_seconds, pending_reclaim
FROM system.cas_mounts WHERE disk = 'cas';

SELECT event_time, outcome, candidates_marked, entries_condemned, entries_graduated,
       entries_redeleted, anomalies
FROM system.cas_gc_log
WHERE event_type = 'Finish' AND disk_name = 'cas'
ORDER BY event_time DESC LIMIT 10;
```

A `0`/`false` `is_leader` means this node never reclaims for this disk — check the peer that holds
leadership instead. A steady `entries_condemned` with `entries_graduated` stuck at `0` means objects
are being found but never crossing the safety floor (recall the grace period is measured in full
rounds, not acks — see [condemnation and deletion](/antalya/cas/architecture/garbage-collection#condemn-delete)).
A specific blob's own story — was it ever condemned, spared, or is it not being seen at all — is the
per-object query in the previous section, filtered to `object_kind = 'blob'`.

### What one GC round did {#gc-round-detail}

Every round writes a `Start` and a `Finish` row to
[`system.cas_gc_log`](/operations/system-tables/cas_gc_log), correlated by `round_id` (not `round`,
which is `0` on `Start` and absent on a round that never led). One `Phase` row per phase reached
carries that phase's own `phase_duration_microseconds`, `ProfileEvents` delta, and `phase_metrics` —
group by `round_id` to reconstruct one round in order:

```sql
SELECT event_type, outcome, phase, phase_duration_microseconds, duration_ms
FROM system.cas_gc_log
WHERE round_id = '<round_id>'
ORDER BY event_time_microseconds;
```

### Who holds the mount {#who-holds-mount}

```sql
SELECT server_root_id, hostname, process_id, state, writer_epoch, renewal_sequence,
       expires_at, is_leader
FROM system.cas_mounts
WHERE disk = 'cas'
ORDER BY is_leader DESC;
```

Every `server_root_id` sharing the pool shows up here, not just this node's own — a `state` other
than `live` (`expired`, `terminated`, `fenced`, `corrupt`) on a member that should be up is the first
thing to check before assuming a lease problem is this node's own. `is_leader` and the other
process-local columns are `NULL` on every peer's row; run the query on that peer to see its own view.

### Trace a renewal through remount {#trace-renewal-remount}

Nontrivial mount recovery is represented by aggregate `watermark_renew` and `mount_remount` rows,
not by one warning per physical request. Query both event types in one timeline:

```sql
SELECT event_time_microseconds, event_type, outcome, reason,
       detail['server_root_id'] AS server_root_id,
       detail['writer_epoch'] AS writer_epoch,
       detail['seq'] AS renewal_sequence,
       detail['write_attempt_id'] AS write_attempt_id,
       detail['attempts_sent'] AS attempts_sent,
       detail['classification'] AS classification,
       detail['deadline_source'] AS deadline_source,
       detail['stop_cause'] AS stop_cause,
       detail['attempt_no'] AS remount_attempt,
       detail['step'] AS remount_step,
       detail['error'] AS error
FROM system.cas_log
WHERE disk_name = 'cas'
  AND event_type IN ('watermark_renew', 'mount_remount')
ORDER BY event_time_microseconds;
```

Interpret the sequence as follows:

- `retrying -> recovered` with the same `write_attempt_id` means an in-budget blip recovered in the
  existing epoch; `classification = 'committed_by_get'` means exact `GET` proved a landed request,
  while `committed_after_retry` means a later identical physical `PUT` completed.
- A `failed` renewal carries the decisive `unresolved_reason`, `deadline_source`, `stop_cause`, and
  `classification`. `external_lease_deadline`, `cancelled`, `conflict`,
  `fence_or_lifecycle_lost`, and `attempts_exhausted` are different operator diagnoses; do not
  collapse them into a generic timeout.
- A following `mount_remount` row names the whole-chain `attempt_no` and final `step`. An `ok` row
  restored `Live` under the reported fresh `writer_epoch`; a `failed` row's `step` and optional
  `error` identify where that whole-chain attempt stopped.

Use deltas of the mount counters from
[monitoring](/antalya/cas/operations/monitoring#mount-renewal-remount-counters) to check completeness:
a recovered blip increments renewal work/recovery but not `CASMountLeaseLost` or remount counters;
a terminal operational loss increments `CASMountLeaseLost` once, then each whole-chain attempt
increments exactly one of `CASRemountSucceeded` or `CASRemountFailed`.

## SQL commands for live diagnosis {#sql-commands}

### SYSTEM CAS FSCK {#sql-fsck}

The online consistency check — unlike the offline tools below, this runs against a disk the server
already has **mounted and serving traffic**; the scan re-validates every finding against a fresh
authoritative read, so it needs no quiesce:

```sql
SYSTEM CAS FSCK cas;
```

Returns one row: `disk`, `reachable`, `dangling`, `unreachable`, `pending_gc`, `awaiting_gc`,
`unaccounted`, `stale_edge`, `corrupted_runs`, `chain_broken`, `unchecked`, `lifeless_keys`,
`namespace_janitor_pending` (+`_bytes`/`_lives`), `ref_records_walked`, `physical_bytes`,
`referenced_logical_bytes`, `distinct_blobs`, `total_blob_refs`. `dangling` is the one column that
means data loss — `unreachable`, `pending_gc`, and `awaiting_gc` are objects still
moving through the normal condemn/graduate/delete pipeline, not a problem on their own.
`chain_broken` and `corrupted_runs` are the other two hard findings: a hole in a ref-log stream and a
GC source-edge run that failed its checksum, respectively. This summary-only form has no
per-object `--detail` equivalent yet — for that, the offline `cas-fsck --detail` below is still
needed.

### SYSTEM CAS GC RUN {#sql-gc-run}

Runs one round synchronously and returns exactly the shape of a `cas_gc_log` `Finish` row — driving
a round on demand while watching its outcome interactively is one of the most direct diagnostics
available:

```sql
SYSTEM CAS GC RUN cas;
```

One row per disk it ran on: `disk`, `acquired_lease`, `deferred`, `round`, `candidates_marked`,
`objects_deleted`, `objects_absent`, `objects_replaced`, `objects_spared`, `manifests_deleted`,
`entries_condemned`, `entries_graduated`, `entries_redeleted`, `fence_outs`, `anomalies`,
`pending_candidates`, `pending_condemned`, `pending_retired`. Omitting
the disk name runs one round on every content-addressed disk on the node. A manual run executes
regardless of `SYSTEM CAS GC STOP` — `STOP` pauses only the background scheduler.

### SYSTEM CAS GC STOP / START {#sql-gc-stop-start}

Pause the background scheduler on one disk while investigating a suspect object, so it cannot be
condemned or deleted mid-investigation, then resume it:

```sql
SYSTEM CAS GC STOP cas;
-- investigate, e.g. cas-inspect a specific blob's raw key
SYSTEM CAS GC START cas;
```

`STOP` is idempotent and stops-in-place (the same scheduler instance resumes on `START`, keeping its
`gc_id` and lease-observation history); it works even on a not-live disk. It does not stop a manual
`SYSTEM CAS GC RUN`. See the [operational surface](/antalya/cas/architecture/garbage-collection#operational-surface)
table for the full command list.

### SYSTEM CAS FORGET {#sql-forget}

Node-local operator assertion that a disk is permanently gone — the "fire marshal" verb for a stuck
disk (a transient/`IdentityLost` pool, an operator-asserted decommission):

```sql
SYSTEM CAS FORGET cas;
```

It is an assertion, not a proof of erasure: the disk stays registered and answers further store-class
access with a typed error, and a server restart re-registers the name. This is different from
[`SYSTEM CAS DROP POOL MEMBER`](/antalya/cas/operations/migration#decommission), which permanently
retires one pool *member*'s identity across the whole shared pool — `FORGET` only affects this node's
own local view of one disk.

## Offline tools {#offline-tools}

When the server cannot answer — it is down, or the access needs to be read-only forensic against the
bucket directly, disaster recovery of `gc/state`, or a raw object decode — `clickhouse-disks` runs
these against the pool's backend without a live server. All five require the disk to be opened with
`<readonly>true</readonly>` in the `clickhouse-disks` config; they must never claim a live server's
mount.

| Command | Use it for |
|---|---|
| `cas-fsck [--detail] [--timeout N] [--namespace PREFIX] [--partial]` | The same reachability scan as `SYSTEM CAS FSCK`, offline. `--detail` adds a per-object `<class>\t<key>\t<size>` listing (`reachable`, `dangling`, `unreachable`, `pending-gc`, `awaiting-gc`, `unaccounted`, `stale-edge`, `corrupted-run`, `chain-broken`, `unchecked`, `lifeless-key`, `janitor-pending`) — the only way to get per-object, not just per-pool, findings. `--timeout`/`--partial` bound a scan on a large pool |
| `cas-gc-dryrun` | Previews the next round's deletes, read-only, no lease. Over-reports away from quiescence (does not fold new owner events) — a diagnostic only, never a delete source |
| `cas-inspect '<raw-object-key>'` | Decodes one raw object-storage key (as printed by `cas-fsck`/`cas-gc-dryrun`) straight to JSON |
| `cas-gc-rebuild [--force]` | Disaster recovery: rebuilds a `gc/state` baseline from raw owner state after the GC guard has refused every regular round. `--force` bypasses only the healthy-state refusal, never a competing leader or a failed `CAS`. See [`SYSTEM CAS GC REBUILD`](/sql-reference/statements/system#system-cas-gc-rebuild) for the destructive-tool caveats |

```bash
clickhouse-disks -C config.xml --disk cas cas-fsck --detail
clickhouse-disks -C config.xml --disk cas cas-gc-dryrun
clickhouse-disks -C config.xml --disk cas cas-inspect '<raw-object-key>'
clickhouse-disks -C config.xml --disk cas cas-gc-rebuild --force
```

`cas-drop-member` — the offline twin of `SYSTEM CAS DROP POOL MEMBER` — is covered on the
[migration page](/antalya/cas/operations/migration#decommission) alongside the SQL form, since
decommissioning a pool member is a migration/scale-down operation, not an incident-time tool.

## The CLICKHOUSE_USER_FILES gotcha when reproducing a test manually {#user-files-gotcha}

Running a `CAS` stateless test directly with `tests/clickhouse-test` against a manually started
`clickhouse-server` (outside a configured praktika lane) requires exporting `CLICKHOUSE_USER_FILES`
to match the server's actual data path. The harness's default,
`/var/lib/clickhouse/user_files`, will not match a custom data path, which makes the pool directory
invisible to the server — the symptom is an `Unknown disk` error together with a diagnostic that
reads like an empty pool (e.g. `baseline=0 after_insert=0`) even though the server is otherwise
healthy.

## What to collect before filing a bug {#filing-a-bug}

- `SYSTEM CAS FSCK '<disk>'` output (or `clickhouse-disks cas-fsck --detail`, if the server cannot
  answer or a per-object listing is needed) — the authoritative reachability snapshot at the time of
  the incident.
- The `system.cas_gc_log` rows for the relevant `round_id`(s): `Start`, every `Phase`, and `Finish`.
- The `system.cas_log` rows for the specific ref name, blob hash, or object key involved, filtered by
  `event_time` around the incident.
- `system.cas_mounts` output from every node sharing the pool, to capture lease/epoch state at
  incident time — it is a live view and will not reflect a state that has since changed.
- For a suspected object-store issue, `system.blob_storage_log` rows for the affected `disk_name`
  with a nonzero `error_code`, and the relevant `CAS*` `ProfileEvents` (`system.query_log`'s
  `ProfileEvents` map for one query, or `system.metric_log`'s `ProfileEvent_*` columns for a window —
  see [monitoring](/antalya/cas/operations/monitoring#key-metrics) for which counters matter and the
  restart-resets-`system.events` caveat).
- The server version and, if the incident is reproducible, the exact `CREATE TABLE` / `INSERT` /
  `ALTER` sequence that triggers it.
