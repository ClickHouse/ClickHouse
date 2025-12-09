#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS dst;
CREATE TABLE dst (id UInt64, data String)
ENGINE=MergeTree()
ORDER BY id
SETTINGS non_replicated_deduplication_window = 1000;
EOF

echo "insert_select_deduplicate=1 with non-stable select, exception expected"
${CLICKHOUSE_CLIENT} --insert_select_deduplicate=1 -q "
    INSERT INTO dst SELECT number AS id, toString(number) AS data FROM numbers(10);
" 2>&1 | grep -o "DEDUPLICATION_IS_NOT_POSSIBLE" | uniq

echo "insert_select_deduplicate=1 with stable select, deduplication happens"
${CLICKHOUSE_CLIENT} --insert_select_deduplicate=1 -q "
    INSERT INTO dst SELECT number AS id, toString(number) AS data FROM numbers(10) ORDER BY all;
    INSERT INTO dst SELECT number AS id, toString(number) AS data FROM numbers(10) ORDER BY all;
    SELECT count() FROM dst;
    TRUNCATE TABLE dst;
"

CH_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/--send_logs_level=[^ ]*//')

echo "insert_select_deduplicate=auto and insert_deduplicate=1 with non-stable select, warning expected, deduplication disabled"
${CH_CLIENT} --insert_select_deduplicate='auto' --insert_deduplicate=1 --send_logs_level='information' -q "
    INSERT INTO dst SELECT number AS id, toString(number) AS data FROM numbers(10);
    INSERT INTO dst SELECT number AS id, toString(number) AS data FROM numbers(10);
" 2>&1 | grep -o "INSERT SELECT deduplication is disabled because SELECT is not stable" | uniq

${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM dst;
    TRUNCATE TABLE dst;
"

echo "insert_select_deduplicate=auto and insert_deduplicate=1 with stable select, deduplication happens"
${CLICKHOUSE_CLIENT} --insert_select_deduplicate='auto' --insert_deduplicate=1 -q "
    INSERT INTO dst SELECT number AS id, toString(number) AS data FROM numbers(10) ORDER BY all;
    INSERT INTO dst SELECT number AS id, toString(number) AS data FROM numbers(10) ORDER BY all;
    SELECT count() FROM dst;
    TRUNCATE TABLE dst;
"

echo "insert_select_deduplicate=0 and insert_deduplicate=1 with stable select, deduplication disabled"
${CLICKHOUSE_CLIENT} --insert_select_deduplicate=0 --insert_deduplicate=1 -q "
    INSERT INTO dst SELECT number AS id, toString(number) AS data FROM numbers(10) ORDER BY all;
    INSERT INTO dst SELECT number AS id, toString(number) AS data FROM numbers(10) ORDER BY all;
    SELECT count() FROM dst;
    TRUNCATE TABLE dst;
"
