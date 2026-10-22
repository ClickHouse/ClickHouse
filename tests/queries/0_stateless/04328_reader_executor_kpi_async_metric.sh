#!/usr/bin/env bash
# Tags: no-distributed-cache, no-encrypted-storage, no-parallel-replicas
# The executor falls back on the distributed cache and decryption (which can't be
# disabled from the test), so its metrics would not be emitted there; skip those
# configs (as in 04316 / 04327).
#   no-parallel-replicas: the counters are incremented on whichever replica reads the
#   mark, so the initiator's `query_log` row does not carry them.
#
# End-to-end check that the modeled-cost KPI asynchronous metric
# `ReaderExecutorModeledCostMsPerRequestedMiB` moves when the executor does work.
# The metric is a ratio of the deltas of `ReaderExecutorModeledCostMicroseconds`
# and `ReaderExecutorDeliveredBytes` over an async-metrics update interval, so we
# bracket a `use_reader_executor` read with two forced updates
# (`SYSTEM RELOAD ASYNCHRONOUS METRICS`) and then assert, in the same time slot,
# that (1) the query recorded a modeled cost in its `query_log` ProfileEvents and
# (2) the asynchronous metric logged a non-zero value.
#
# The KPI is sampled per update interval, instance-wide, and is only observable
# through the flushed `asynchronous_metric_log`, so a single probe races with the
# periodic update thread: the interval that carries the read's delta can be closed
# by a background tick whose row lands in the log queue after the `SYSTEM FLUSH
# LOGS` snapshot, leaving only zero-valued rows in the window. The probe is
# therefore repeated until the sample lands; `max(value)` over a growing window is
# monotonic, so every extra attempt is another chance and never invalidates an
# earlier one.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

PROBE="04328_reader_executor_kpi_probe_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS t_reader_executor_kpi;

CREATE TABLE t_reader_executor_kpi
(
    id UInt64,
    v UInt64,
    s String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO t_reader_executor_kpi
SELECT number, number * 2, concat('row_', toString(number))
FROM numbers(300000);
"

cost_recorded=0
metric_logged=0

for _ in {1..20}
do
    $CLICKHOUSE_CLIENT -m -q "
    SET use_reader_executor = 1;
    SET remote_filesystem_read_method = 'read';
    SET enable_filesystem_cache = 0;

    -- Baseline tick: establishes the previous-counter snapshot (and clears first_run),
    -- so the next update measures the delta produced by the read below.
    SYSTEM RELOAD ASYNCHRONOUS METRICS;

    -- The load. Its result is irrelevant (it only drives the executor).
    SELECT count(), sum(id), sum(v), sum(length(s)) FROM t_reader_executor_kpi
    SETTINGS log_comment = '$PROBE' FORMAT Null;

    -- Tick that captures the read's delta into the KPI and logs it.
    SYSTEM RELOAD ASYNCHRONOUS METRICS;

    SYSTEM FLUSH LOGS query_log, asynchronous_metric_log;
    "

    # (1) The query itself recorded a non-zero modeled cost.
    cost_recorded=$($CLICKHOUSE_CLIENT -q "
    SELECT ProfileEvents['ReaderExecutorModeledCostMicroseconds'] > 0
    FROM system.query_log
    WHERE log_comment = '$PROBE'
      AND type = 'QueryFinish'
      AND current_database = currentDatabase()
    ORDER BY event_time_microseconds DESC
    LIMIT 1")

    # (2) In the same time slot (at or after the first probe started), the asynchronous
    # KPI metric logged a non-zero value. Scoping by the probe's start time excludes the
    # baseline tick of the first attempt (which ran before any read).
    metric_logged=$($CLICKHOUSE_CLIENT -q "
    SELECT max(value) > 0
    FROM system.asynchronous_metric_log
    WHERE metric = 'ReaderExecutorModeledCostMsPerRequestedMiB'
      AND event_time >= (
          SELECT min(query_start_time)
          FROM system.query_log
          WHERE log_comment = '$PROBE'
            AND current_database = currentDatabase()
      )")

    [[ "$cost_recorded" == 1 && "$metric_logged" == 1 ]] && break
done

echo "$cost_recorded"
echo "$metric_logged"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_reader_executor_kpi"
