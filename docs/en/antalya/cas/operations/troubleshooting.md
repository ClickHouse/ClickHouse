---
description: 'Symptom-to-action table for common content-addressed storage incidents: mount lease loss, stalled GC, startup failures, fsck timeouts, and read-only pools.'
sidebar_label: 'Troubleshooting'
sidebar_position: 3
slug: /antalya/cas/operations/troubleshooting
title: 'CAS Operations — Troubleshooting'
doc_type: 'guide'
---

# Operations — troubleshooting {#troubleshooting}

Start from the symptom, not the mechanism. Each row below names a concrete diagnostic query or
command and the action it points to; see [monitoring](/antalya/cas/operations/monitoring) for the
system tables referenced and [debugging](/antalya/cas/operations/debugging) for the underlying
tools.

| Symptom | Diagnosis | Action |
|---|---|---|
| A server keeps losing its mount lease and self-remounting | Check `system.cas_mounts` for the server's own `state`/`expires_at`, then correlate `watermark_renew` and `mount_remount` in `system.cas_log`; losing the lease trips a local fence and latches a remount generation | Read the failed renewal's `classification` before changing anything — it alone now says why (see [the decision flow](#mount-renewal-remount-flow)). Look for object-store latency consuming the confirmed lease or BOOTTIME advancement; see [the mount lease](/antalya/cas/architecture/mounts-and-leases#mount-lease) |
| Writes slow down or stall under load, with no exception reaching the client | S3 `SlowDown`/`ServiceUnavailable`/`RequestTimeout`/`InternalError` (5xx) responses are not on the request engine's `isDefinitelyRefusedWrite` definite-failure list (only malformed-request, entity-too-large, and access-denied that no credential refresh can fix are), so they classify as ambiguous and are retried automatically. Confirm with `sum(ProfileEvents['CASConditionalWriteUnresolved'])` rising alongside `sum(ProfileEvents['CASConditionalWriteAttempts'])` over `system.query_log` for the affected window (or `ProfileEvent_CASConditionalWriteUnresolved` in `system.metric_log` for a cumulative view across queries), and check `system.blob_storage_log` for `disk_name = '<cas>'` rows with a nonzero `error_code` around the same window | Nothing to configure per-request: the request engine retries the same `(key, bytes)` with capped-exponential backoff (200ms initial, capped at 5s, full jitter) until the 90-second operation deadline — there is no separate attempts ceiling, only the deadline — and the mount-lease renewer keeps extending the fence across the disruption — this is the "blips, throttling, partial outages" case the write path is built to survive. Confirm the mount lease itself is still renewing (`system.cas_mounts.expires_at` moving forward, `last_success_age_seconds` not climbing) — if it is, this is expected and self-resolving. If `SlowDown` responses are sustained rather than transient, check the bucket's request-rate limits against the pool's actual PUT/GET rate (see [bucket requirements](/antalya/cas/bucket-requirements)) and consider lowering `cas_blob_upload_pool_size` to reduce concurrent upload traffic; a write only surfaces a client-visible `NETWORK_ERROR` if the 90-second deadline is exhausted before the store recovers, and that error is retried by the ordinary merge/insert backoff, not silently dropped |
| `GC` never seems to reclaim space after tables are dropped | `SELECT * FROM system.cas_gc_log WHERE event_type='Finish' ORDER BY event_time DESC LIMIT 5` — check `outcome`; also `SELECT is_leader FROM system.cas_mounts` on this node | If `outcome != 'Success'`/`'Deferred'`, see [reading GC health](/antalya/cas/operations/monitoring#gc-health); if this node is not the leader (`is_leader = 0`), it never reclaims for this disk — check the peer holding leadership. Reclamation also needs at least two full rounds past condemnation by design (the grace period is rounds, not acks) — a single manual `SYSTEM CAS GC RUN` will not finish it |
| A dangling-access exception or `CORRUPTED_DATA` on read | Run `clickhouse-disks cas-fsck --detail` and check `dangling` specifically — it is the one class that means data loss, distinct from `unreachable`/`awaiting-gc`, which are just waiting for graduation | A nonzero `dangling` count is a real incident: collect the `--detail` output (see [what to collect before filing a bug](/antalya/cas/operations/debugging#filing-a-bug)) before taking any destructive action |
| `SYSTEM CAS FSCK` or `clickhouse-disks cas-fsck` times out on a large pool | The scan is bounded by `--timeout` (default 600s / the `SYSTEM` form has no override); a large `roots/` prefix can make the scan slow | Retry with `--partial` to see the counts accumulated so far instead of aborting empty-handed, or `--namespace <prefix>` to scope the scan to a subset of namespaces |
| `SYSTEM CAS DROP POOL MEMBER` returns a non-empty `warnings` column | A per-object drain step could not confirm emptiness; the mount slot is left terminated but not fully drained, as a resume anchor | Rerun the same command — it is resumable and skips namespaces already marked removed, reporting them under `namespaces_already_removed` |
| Writes or `ALTER`s on a `CAS` disk fail with a `READONLY`-class error | The disk's metadata storage rejects every mutating entry point; this is deliberate for a disk opened with `<readonly>true</readonly>`, used by every offline `clickhouse-disks` tool | Confirm whether the disk was intentionally configured read-only (offline inspection, `cas-fsck`, `cas-gc-dryrun`, `cas-gc-rebuild`, `cas-drop-member` all require it); a production disk serving writes must not carry `<readonly>true</readonly>` |
| A table stays unavailable after a transient network error during startup | `AsyncLoader` has no retry/requeue path for a failed table load job: a transient S3 `NETWORK_ERROR` during `CAS` ref-table startup recovery can leave the job permanently `FAILED` | Restart the server, or issue a fresh load for the table; this is a one-shot job design, not a `CAS`-specific bug |
| A mounted pool directory was removed or renamed out of band | Renewal observes an absent, foreign, successor, or otherwise conflicting mount body and terminates the keeper with a typed fail-closed exception; the runtime closes the local write fence and requests remount rather than adopting the body | Never remove or rename a live pool's storage path. To retire a member permanently use [`SYSTEM CAS DROP POOL MEMBER`](/antalya/cas/operations/migration#decommission) instead of raw filesystem operations; collect the `watermark_renew` classification (`vanished` for an absent body, `conflict` for a foreign, successor, or otherwise conflicting one) and the subsequent `mount_remount` step |
| Stale-looking part metadata after an out-of-band change to the pool | The part-folder view cache may be serving a retained (not re-validated) view | Set the disk-level `cas_part_folder_cache_bytes = 0` to disable view retention and `cas_manifest_decode_cache_bytes = 0` to make every manifest read fetch the body, then run `fsck`; both are diagnostic kill switches, not steady-state settings |
| A wide merge (many thousands of columns) fails with a port-exhaustion error from the network layer | Each column in a wide part can cost a separate object-store operation in one merge, and a very wide part can issue on the order of the column count in requests, exhausting local ephemeral TCP ports under load | Reduce concurrent merge parallelism on that table, or increase the host's ephemeral port range; this is a general high-fan-out-merge limit, not specific to content addressing |

## Mount renewal and remount decision flow {#mount-renewal-remount-flow}

Start with the `watermark_renew` timeline described in
[debugging](/antalya/cas/operations/debugging#trace-renewal-remount), then follow the matching case:

1. **Recovered blip.** A single `outcome = 'recovered'` row (there is no separate `retrying` row to
   look for) with `classification` of `committed_by_read` or `committed_after_retry`;
   `CASMountRenewalRecovered` rises while `CASMountLeaseLost` and all remount counters stay flat. No
   intervention is needed unless the rate is sustained; investigate backend throttling/latency before
   the blips consume the lease budget.
2. **External lease-safety exhaustion.** The failed row has
   `classification = 'external_lease_deadline'`; `CASMountRenewalDeadlineExceeded` and
   `CASMountLeaseLost` rise. The runtime correctly refused to manufacture authority beyond the last
   confirmed lease. Check object-store latency and BOOTTIME/suspend history, then follow the ensuing
   remount. `classification = 'request_deadline'` is the sibling case: the ninety-second request
   policy exhausted first rather than the lease's own safety margin.
3. **Cancellation.** `classification = 'cancelled'` after a sent request is terminal and suppresses a
   clean farewell because the request may still land. Cancellation before any request remains
   `Active` and emits no failed aggregate row; during graceful shutdown that is the expected
   clean-release path.
4. **Confirmed conflict.** `classification = 'conflict'` means exact resolution found another body;
   inspect `server_root_id`, `writer_epoch`, `seq`, and `write_attempt_id`. Same-pair twins, GC-fenced
   bodies, successor epochs, and foreign holders all remain fail closed. Do not delete or rewrite the
   mount key by hand.
5. **Fence or lifecycle loss.** `classification = 'fence_or_lifecycle_lost'` means another local loss,
   remount park request, or terminal lifecycle closed admission while the operation was active. A
   parked result reuses the already-requested recovery generation and must not double-count
   `CASMountLeaseLost`. `classification = 'unresolved'` is a related but distinct case: every attempt
   stayed ambiguous and the operation gave up without ever settling one way or the other.
6. **Whole-chain remount failure.** Read the following `mount_remount` row. Its `attempt_no`, `step`,
   and optional `error` identify the failed owner/catalog/epoch/claim/install/quiescence/fence step.
   The current protocol retries the whole chain with bounded backoff; it does not preserve per-step
   progress. Repeated failure at the same step is the actionable signal.

The default-level log policy is intentionally bounded: one warning on the first transition to retry,
then one recovery info or terminal fence warning, plus one final line per whole-chain remount attempt.
Use `system.cas_log` and counter deltas to reconstruct the incident; `DEBUG` contains individual
physical retries when that extra transport detail is necessary.
