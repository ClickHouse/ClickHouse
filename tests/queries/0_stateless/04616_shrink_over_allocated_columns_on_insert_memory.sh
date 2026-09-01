#!/usr/bin/env bash
# Tags: long, no-fasttest, no-asan, no-msan, no-tsan, no-ubsan, no-random-settings
# Checks that shrink_over_allocated_columns_min_waste_ratio reduces INSERT peak memory: it shrinks
# over-allocated columns (e.g. String columns grown power-of-two while parsing) to fit before the part
# is written, so the over-allocated original block is not copied wholesale during the pre-write permute.
# The effect is largest for Compact parts (whole-block permute) with a non-monotonic sort key, so we
# insert two big over-allocated String columns server-side over HTTP into a Compact table ordered by an
# unsorted key. Sanitizers inflate memory and randomized settings change the buffers/limits, so those
# are excluded.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/04616_shrink_${CLICKHOUSE_DATABASE}.rowbinary"

# ~515 MB of RowBinary (id UInt64, c1 String, c2 String): 270 rows, each String value ~1 MB, so each
# String column holds ~257 MiB of chars -- just above the 256 MiB power-of-two boundary, so parsing
# over-allocates it to ~512 MiB (~255 MiB wasted per column). id = cityHash64(number) is unsorted, so
# ORDER BY id forces a real permutation of the two big columns.
${CLICKHOUSE_LOCAL} -q "
    SELECT cityHash64(number) AS id, repeat('x', 999000) AS c1, repeat('y', 999000) AS c2
    FROM numbers(270)
    INTO OUTFILE '${DATA_FILE}' FORMAT RowBinary"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_shrink_mem"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_shrink_mem (id UInt64, c1 String, c2 String)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 10737418240, min_rows_for_wide_part = 100000000"

QUERY="INSERT INTO t_shrink_mem FORMAT RowBinary"
ENC_QUERY=$(python3 -c "import urllib.parse, sys; print(urllib.parse.quote(sys.argv[1]))" "$QUERY")

suffix="${CLICKHOUSE_DATABASE}_${RANDOM}"
qid_off="shrink_off_${suffix}"
qid_on="shrink_on_${suffix}"

# Shrinking disabled (ratio 1.0).
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=${qid_off}&query=${ENC_QUERY}&shrink_over_allocated_columns_min_waste_ratio=1.0" \
    --data-binary @"${DATA_FILE}"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t_shrink_mem"

# Shrinking enabled (ratio 1.5).
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=${qid_on}&query=${ENC_QUERY}&shrink_over_allocated_columns_min_waste_ratio=1.5" \
    --data-binary @"${DATA_FILE}"

rm -f "${DATA_FILE}"

# The query_log entry is written asynchronously, after the HTTP response is sent
# (https://github.com/ClickHouse/ClickHouse/issues/84364), so a single FLUSH LOGS races the
# log write. Retry FLUSH until both QueryFinish rows have landed.
for _ in {1..60}; do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
    landed=$(${CLICKHOUSE_CLIENT} -q "
        SELECT countIf(query_id = '$qid_off') > 0 AND countIf(query_id = '$qid_on') > 0
        FROM system.query_log
        WHERE type = 'QueryFinish' AND event_date >= yesterday() AND current_database = currentDatabase()")
    [ "$landed" = "1" ] && break
    sleep 0.5
done

# Shrinking must reduce peak memory by more than 10% (measured ~17%).
${CLICKHOUSE_CLIENT} -q "
    WITH
        (SELECT memory_usage FROM system.query_log
         WHERE query_id = '$qid_off' AND type = 'QueryFinish' AND event_date >= yesterday() AND current_database = currentDatabase()
         ORDER BY event_time DESC LIMIT 1) AS off_mem,
        (SELECT memory_usage FROM system.query_log
         WHERE query_id = '$qid_on' AND type = 'QueryFinish' AND event_date >= yesterday() AND current_database = currentDatabase()
         ORDER BY event_time DESC LIMIT 1) AS on_mem
    SELECT (off_mem - on_mem) / off_mem > 0.1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_shrink_mem"
