#!/usr/bin/env bash
# Tags: long, no-fasttest, no-asan, no-msan, no-tsan, no-ubsan, no-random-settings
# Checks that input_format_json_max_string_column_growth_step reduces INSERT peak memory: it caps the
# power-of-two growth of the JSON column's internal String buffers built while materializing JSON.
# The effect is only visible when the JSON build is the peak, so we parse the data server-side over
# HTTP via input() and cast it to JSON (no permute, insert into a Null table).
# Sanitizers inflate memory and randomized settings change the buffers/limits, so those are excluded.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/04615_json_growth_cap_${CLICKHOUSE_DATABASE}.rowbinary"

# ~600 MB of RowBinary (data String): 272 rows x ~2.2 MB JSON docs (500 fields, each a 4400-char string
# value). With max_dynamic_paths=0 every field overflows into shared data, so the shared-data value
# buffer (built via a doubling WriteBufferFromVector) grows into the 512 MiB..1 GiB power-of-two bucket
# and the cap has a large, stable effect.
${CLICKHOUSE_LOCAL} -q "
    SELECT
        concat('{', arrayStringConcat(arrayMap(i -> concat('\"f', toString(i), '\":\"', repeat('x', 4400), '\"'), range(500)), ','), '}') AS data
    FROM numbers(272)
    INTO OUTFILE '${DATA_FILE}' FORMAT RowBinary"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_json_growth_cap"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_json_growth_cap (Document JSON(max_dynamic_paths=0)) ENGINE = Null"

QUERY="INSERT INTO t_json_growth_cap SELECT data::JSON(max_dynamic_paths=0) FROM input('data String') FORMAT RowBinary"
ENC_QUERY=$(python3 -c "import urllib.parse, sys; print(urllib.parse.quote(sys.argv[1]))" "$QUERY")

suffix="${CLICKHOUSE_DATABASE}_${RANDOM}"
qid_off="json_growth_cap_off_${suffix}"
qid_on="json_growth_cap_on_${suffix}"

# Cap disabled (pure power-of-two doubling).
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=${qid_off}&query=${ENC_QUERY}&input_format_json_max_string_column_growth_step=0" \
    --data-binary @"${DATA_FILE}"

# Cap enabled (grow by 128 MiB increments once past the step).
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=${qid_on}&query=${ENC_QUERY}&input_format_json_max_string_column_growth_step=134217728" \
    --data-binary @"${DATA_FILE}"

rm -f "${DATA_FILE}"

# Retry loop to handle the race between the HTTP response and the query_log entry being written.
for _ in {1..60}; do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
    count=$(${CLICKHOUSE_CLIENT} -q "
        SELECT count() FROM system.query_log
        WHERE query_id IN ('$qid_off', '$qid_on') AND type = 'QueryFinish'
          AND event_date >= yesterday() AND current_database = currentDatabase()")
    [ "$count" -ge 2 ] && break
    sleep 0.5
done

# The cap must reduce peak memory by more than 10% (measured ~15%).
${CLICKHOUSE_CLIENT} -q "
    WITH
        (SELECT memory_usage FROM system.query_log
         WHERE query_id = '$qid_off' AND type = 'QueryFinish' AND event_date >= yesterday() AND current_database = currentDatabase()
         ORDER BY event_time DESC LIMIT 1) AS off_mem,
        (SELECT memory_usage FROM system.query_log
         WHERE query_id = '$qid_on' AND type = 'QueryFinish' AND event_date >= yesterday() AND current_database = currentDatabase()
         ORDER BY event_time DESC LIMIT 1) AS on_mem
    SELECT (off_mem - on_mem) / off_mem > 0.1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_json_growth_cap"
