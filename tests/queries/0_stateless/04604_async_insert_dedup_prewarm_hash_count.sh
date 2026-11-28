#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression guard for DeduplicationInfo::prewarmDataHashes (PR #111150).
#
# Async-insert deduplication computes a column-wise data hash per token. When a coalesced
# flush is split by partition, each token that has rows in a partition is (re)hashed while
# processing that partition. A token whose one entry spans P partitions is therefore hashed
# once per partition it lands in -> O(P) for that token, O(P*N) for the whole flush.
#
# prewarmDataHashes computes each token's hash once, before the partition loop, so the
# per-partition infos reuse the cached value: O(N) total regardless of how many partitions a
# token spans. This is the case master's filterToPartition cannot reduce (a token that spans
# every partition is kept in every partition), so it is exactly where prewarm still matters.
#
# We drive N async entries, each spanning all P partitions, and assert the flush computed
# each token's hash exactly once (N in total), not once per partition (P*N). The count is
# read from the flushes' own system.query_log rows via asynchronous_insert_log.flush_query_id,
# so the check is deterministic (an exact count, not a timing) and needs no no-parallel tag.

N=30
P=4

# system.asynchronous_insert_log is append-only and keyed only by database/table below, which
# do not change across reruns on the same server (e.g. a manual local re-invocation, as opposed
# to CI's per-test-run randomized database) -- so a bare database/table filter would also match
# every past run's flush_query_id, silently inflating the expected count on rerun. Capture a
# precise lower time bound before any insert runs so the lookup is scoped to this execution only.
# The bound must be compared against flush_time_microseconds: flush_time is a second-granularity
# DateTime, so when the whole test fits in one wall-clock second it truncates to BEFORE the
# sub-second start_time and the filter would drop this run's own flush.
start_time=$($CLICKHOUSE_CLIENT -q "SELECT now64(6)")

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_dedup_prewarm"
$CLICKHOUSE_CLIENT -q "
CREATE TABLE t_dedup_prewarm (p UInt8, v UInt64)
ENGINE = MergeTree
PARTITION BY p
ORDER BY v
SETTINGS non_replicated_deduplication_window = 100000"

# N entries; each entry writes one row into every partition (p = 0..P-1) and carries a value
# unique to the entry, so every token spans all P partitions and nothing is deduplicated.
query=""
for ((i = 0; i < N; i++)); do
    query+="INSERT INTO t_dedup_prewarm VALUES"
    for ((p = 0; p < P; p++)); do
        [ "$p" -gt 0 ] && query+=","
        query+=" ($p, $i)"
    done
    query+=";"
done

# The query is generated; keep it in the reference so a failing diff shows what was inserted.
echo "$query"

# Fire them all without waiting, and keep them in a single batch: disable the adaptive/busy
# timeout flush and raise the size/count thresholds, so nothing flushes until we ask.
# All inserts share one query+settings key, so they land in one shard and one flush.
$CLICKHOUSE_CLIENT \
    --async_insert 1 \
    --wait_for_async_insert 0 \
    --async_insert_deduplicate 1 \
    --async_insert_use_adaptive_busy_timeout 0 \
    --async_insert_busy_timeout_max_ms 600000 \
    --async_insert_max_query_number 1000000 \
    --async_insert_max_data_size 1000000000 \
    -n -q "$query"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH ASYNC INSERT QUEUE t_dedup_prewarm"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log, asynchronous_insert_log"

# Sum DuplicationDataHashComputations over every flush that ingested our entries. With prewarm
# each of the N tokens is hashed exactly once, so the total is N even though each token spans
# P partitions; without prewarm it would be P*N.
$CLICKHOUSE_CLIENT -q "
SELECT 'data_hash_computations', sum(ProfileEvents['DuplicationDataHashComputations'])
FROM system.query_log
-- AND current_database = currentDatabase() -- Just to silence style check: the flush runs with default values, so it is scoped via flush_query_id below.
WHERE type = 'QueryFinish' AND query_id IN (
    SELECT DISTINCT flush_query_id
    FROM system.asynchronous_insert_log
    WHERE database = currentDatabase() AND table = 't_dedup_prewarm' AND status = 'Ok'
      AND flush_time_microseconds >= toDateTime64('$start_time', 6)
)"

# Sanity: all N*P distinct rows are present (none wrongly deduplicated).
$CLICKHOUSE_CLIENT -q "SELECT 'rows', count() FROM t_dedup_prewarm"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_dedup_prewarm"
