#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Verifies that `max_bytes_ratio_before_external_limit_by` triggers spilling on its own, with no
# absolute threshold set. This is the automatic half of the feature: the other tests pin
# `max_bytes_before_external_limit_by`, which only proves the spill machinery works once someone
# asks for it.
#
# The ratio is resolved against the memory still available to the *user*, not to the query:
# `getMostStrictAvailableSystemMemory` starts from the parent of the query-level tracker, so a
# `SET max_memory_usage` in a plain .sql test would be ignored and the query would silently stay in
# memory. Hence the dedicated user below.

USER="u05143_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER} IDENTIFIED WITH no_password SETTINGS max_memory_usage_for_user = '1Gi'"
$CLICKHOUSE_CLIENT -q "GRANT ALL ON *.* TO ${USER}"

LOG_COMMENT="05143_ratio_${CLICKHOUSE_DATABASE}"

# 0.0001 of ~1 GiB is roughly 100 KiB, well below what any query of this size occupies, so the
# ratio alone is enough to start spilling. Every key appears exactly 3 times, so `LIMIT 2 BY` has to
# return exactly 2 rows for each of the 100000 groups no matter which rows the spill picks up first.
# `max_threads` matters beyond speed here: spilling is only chosen when `LIMIT BY` has more than one
# input stream, and `max_block_size` keeps the source from collapsing into a single one.
$CLICKHOUSE_CLIENT --user "${USER}" -q "
    SELECT min(c), max(c), count()
    FROM
    (
        SELECT k, count() AS c
        FROM
        (
            SELECT number % 100000 AS k, number AS v
            FROM numbers_mt(300000)
            LIMIT 2 BY k
        )
        GROUP BY k
    )
    SETTINGS
        max_threads = 4,
        max_block_size = 8192,
        max_bytes_before_external_limit_by = 0,
        max_bytes_ratio_before_external_limit_by = 0.0001,
        log_comment = '${LOG_COMMENT}'
"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

# Without this the assertion above would still pass if the ratio had been ignored and the whole
# `LIMIT BY` had run in memory. The byte counters are checked against a real magnitude rather than
# just being non-zero, so that a spill that writes only an empty run does not count as one, and the
# generic `ExternalProcessing*` counters confirm the temporary data went through the shared
# accounting rather than bypassing it.
$CLICKHOUSE_CLIENT -q "
    SELECT
        countIf(ProfileEvents['ExternalLimitByWritePart'] > 0) > 0 AS wrote_runs,
        countIf(ProfileEvents['ExternalLimitByMerge'] > 0) > 0 AS merged_runs,
        countIf(ProfileEvents['ExternalLimitByUncompressedBytes'] >= 100000) > 0 AS wrote_bytes,
        countIf(ProfileEvents['ExternalLimitByCompressedBytes'] > 0) > 0 AS compressed_bytes,
        countIf(ProfileEvents['ExternalProcessingFilesTotal'] > 0) > 0 AS counted_as_external,
        countIf(ProfileEvents['ExternalProcessingUncompressedBytesTotal'] >= 100000) > 0 AS counted_bytes_as_external
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND log_comment = '${LOG_COMMENT}'
        AND type = 'QueryFinish'
        AND event_date >= yesterday()
"

# The temporary files are also reported as a live metric. Only its presence is checked: the metric
# is sampled on a timer and may well read zero by the time the query is over.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS metric_log"
$CLICKHOUSE_CLIENT -q "
    SELECT CurrentMetric_TemporaryFilesForLimitBy
    FROM system.metric_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
    ORDER BY event_time DESC
    LIMIT 1
    FORMAT Null
"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER}"
