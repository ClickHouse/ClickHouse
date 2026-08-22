#!/usr/bin/env bash
# Tags: no-parallel, no-parallel-replicas, no-fasttest
# no-fasttest: the failpoints require a build with libfiu.
# no-parallel: the failpoints are server-global, and another streaming test can consume a one-shot.
# no-parallel-replicas: a streaming read is served locally, so it is not meaningful across replicas.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

FP_ROUND=streaming_enrichment_pause_before_enrichment
FP_OBSERVED=streaming_bounded_pause_after_snapshot_observed

# An armed failpoint holds the table's enrichment, which blocks DROP TABLE, so always release.
trap '$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT '"$FP_ROUND"';
    SYSTEM DISABLE FAILPOINT '"$FP_OBSERVED"';
" 2>/dev/null || true' EXIT

# One reader, so every partition belongs to it.
STREAM_SETTINGS="--enable_analyzer 1 --enable_streaming_queries 1 --use_skip_indexes_on_data_read 0 --max_threads 1"

TABLE_SETTINGS="
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset'"

# A bounded stream reads how much has been published and what is readable separately, so a whole
# round can land between the two. Reading the count first bounds what the second read can be.

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_bounded_between;
CREATE TABLE t_bounded_between (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k SETTINGS $TABLE_SETTINGS;"

# Armed before any reader exists, so the pump spends the one-shot pause and the reader below, which
# is the one being measured, gets a fresh one instead of racing the pump for it.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP_ROUND"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP_OBSERVED"

# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $STREAM_SETTINGS --max_execution_time 30 \
    --query_id "${CLICKHOUSE_DATABASE}_between_pump" --query \
    "SELECT 'pump', count() FROM t_bounded_between STREAM BOUNDED" > /dev/null 2>&1 &
PUMP=$!

timeout 30 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP_ROUND PAUSE" \
    || echo "observed_between initial round barrier timed out"
timeout 30 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP_OBSERVED PAUSE" \
    || echo "observed_between pump barrier timed out"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_OBSERVED" 2>/dev/null || true

$CLICKHOUSE_CLIENT --query "
INSERT INTO t_bounded_between SELECT number, number * 10 FROM numbers(5);
INSERT INTO t_bounded_between SELECT number, number * 10 FROM numbers(5, 5);"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP_OBSERVED"

# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $STREAM_SETTINGS --max_execution_time 30 \
    --query_id "${CLICKHOUSE_DATABASE}_between_reader" --query \
    "SELECT 'observed_between', count(), sum(k), sum(v) FROM t_bounded_between STREAM BOUNDED" &
READER=$!

# Stop this reader once both its reads are taken, both against the still-empty table.
timeout 30 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP_OBSERVED PAUSE" \
    || echo "observed_between reader barrier timed out"

# With the reader held, step two whole rounds. The first was parked while the table was empty and
# does not count for this reader; the second does. Each wait is bounded and announces a timeout,
# because a build that reads in the wrong order leaves no round to park.
$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT $FP_ROUND" 2>/dev/null || true
timeout 30 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP_ROUND PAUSE" \
    || echo "observed_between first round barrier timed out"
$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT $FP_ROUND" 2>/dev/null || true
timeout 30 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP_ROUND PAUSE" \
    || echo "observed_between second round barrier timed out"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_ROUND" 2>/dev/null || true

# Releasing the reader now makes it act on that pair: the right order comes back for the rows, the
# wrong one stops having read nothing.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_OBSERVED" 2>/dev/null || true

wait $READER
wait $PUMP || true
$CLICKHOUSE_CLIENT --query "DROP TABLE t_bounded_between"

# Carriers that must keep working: an empty table has nothing to wait for and still has to finish,
# and the other bounded modifiers must read the same rows.

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_bounded_carriers;
CREATE TABLE t_bounded_carriers (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k SETTINGS $TABLE_SETTINGS;"

# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $STREAM_SETTINGS --query \
    "SELECT 'empty', count() FROM t_bounded_carriers STREAM BOUNDED"

$CLICKHOUSE_CLIENT --query "
INSERT INTO t_bounded_carriers SELECT number, number * 10 FROM numbers(5);
INSERT INTO t_bounded_carriers SELECT number, number * 10 FROM numbers(5, 5);"

# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $STREAM_SETTINGS --query "
SELECT 'bounded', count(), sum(k), sum(v) FROM t_bounded_carriers STREAM BOUNDED;
SELECT 'bounded_unordered', count(), sum(k), sum(v) FROM t_bounded_carriers STREAM BOUNDED UNORDERED;
SELECT 'bounded_cursor_from_start', count(), sum(k), sum(v) FROM t_bounded_carriers STREAM BOUNDED CURSOR {'all': {'block_number': 0}};"

# A cursor resolves against the block numbers actually assigned, so read them rather than assume.
FIRST_BLOCK=$($CLICKHOUSE_CLIENT --query \
    "SELECT min(min_block_number) FROM system.parts WHERE database = currentDatabase() AND table = 't_bounded_carriers' AND active")
LAST_BLOCK=$($CLICKHOUSE_CLIENT --query \
    "SELECT max(max_block_number) FROM system.parts WHERE database = currentDatabase() AND table = 't_bounded_carriers' AND active")

# Resuming past the first block skips its rows; past every block leaves nothing, and must finish.
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $STREAM_SETTINGS --query "
SELECT 'bounded_cursor_resumed', count(), sum(k), sum(v) FROM t_bounded_carriers
    STREAM BOUNDED CURSOR {'all': {'block_number': $FIRST_BLOCK, 'block_offset': 4}};
SELECT 'bounded_cursor_exhausted', count(), sum(k), sum(v) FROM t_bounded_carriers
    STREAM BOUNDED CURSOR {'all': {'block_number': $((LAST_BLOCK + 1))}};"

# LIMIT 1 emits a row first, so this covers being shut down after reading; the last arm, before.
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $STREAM_SETTINGS --max_execution_time 30 --query \
    "SELECT 'cancelled', count() FROM (SELECT k FROM t_bounded_carriers STREAM BOUNDED LIMIT 1)"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_bounded_carriers"

# A stream shut down before any data reached it must still finish.

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_bounded_disabled;
CREATE TABLE t_bounded_disabled (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k SETTINGS $TABLE_SETTINGS;
INSERT INTO t_bounded_disabled SELECT number, number * 10 FROM numbers(5);"

# Armed before the reader exists, so it reaches its decision with nothing ever published to it.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP_ROUND"

# LIMIT 0 shuts the reader down at once and nothing else cancels the query, so if it does not finish
# itself it never does. max_execution_time reports that as a wrong value, not a silent harness timeout.
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $STREAM_SETTINGS --max_execution_time 30 --query \
    "SELECT 'disabled_before_round', count() FROM (SELECT k FROM t_bounded_disabled STREAM BOUNDED LIMIT 0)"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP_ROUND" 2>/dev/null || true
$CLICKHOUSE_CLIENT --query "DROP TABLE t_bounded_disabled"
