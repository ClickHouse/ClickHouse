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

# async_insert=0 -- it is insert select queries no need to async inserts
# insert_deduplicate=1 -- to enable deduplication feature
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --async_insert=0 --insert_deduplicate=1"

echo "deduplicate_insert_select=force_enable with non-stable select, exception expected"
${CLICKHOUSE_CLIENT} --deduplicate_insert_select=force_enable -q "
    INSERT INTO dst SELECT number AS id, toString(number) AS data FROM numbers(10);
" 2>&1 | grep -o "DEDUPLICATION_IS_NOT_POSSIBLE" | uniq

echo "deduplicate_insert_select=force_enable with stable select, deduplication happens"
${CLICKHOUSE_CLIENT} --deduplicate_insert_select=force_enable -q "
    INSERT INTO dst SELECT number AS id, toString(number) AS data FROM numbers(10) ORDER BY all;
    INSERT INTO dst SELECT number AS id, toString(number) AS data FROM numbers(10) ORDER BY all;
    SELECT count() FROM dst;
    TRUNCATE TABLE dst;
"

CH_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/--send_logs_level=[^ ]*//')

echo "deduplicate_insert_select=enable_when_possible and insert_deduplicate=1 with non-stable select, warning expected, deduplication disabled"
${CH_CLIENT} --deduplicate_insert_select='enable_when_possible' --send_logs_level='information' -q "
    INSERT INTO dst SELECT number AS id, toString(number) AS data FROM numbers(10);
    INSERT INTO dst SELECT number AS id, toString(number) AS data FROM numbers(10);
" 2>&1 | grep -o "INSERT SELECT deduplication is disabled because SELECT is not stable" | uniq

${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM dst;
    TRUNCATE TABLE dst;
"

echo "deduplicate_insert_select=enable_when_possible and insert_deduplicate=1 with stable select, deduplication happens"
${CLICKHOUSE_CLIENT} --deduplicate_insert_select='enable_when_possible' -q "
    INSERT INTO dst SELECT number AS id, toString(number) AS data FROM numbers(10) ORDER BY all;
    INSERT INTO dst SELECT number AS id, toString(number) AS data FROM numbers(10) ORDER BY all;
    SELECT count() FROM dst;
    TRUNCATE TABLE dst;
"

echo "deduplicate_insert_select=disable and insert_deduplicate=1 with stable select, deduplication disabled"
${CLICKHOUSE_CLIENT} --deduplicate_insert_select=disable -q "
    INSERT INTO dst SELECT number AS id, toString(number) AS data FROM numbers(10) ORDER BY all;
    INSERT INTO dst SELECT number AS id, toString(number) AS data FROM numbers(10) ORDER BY all;
    SELECT count() FROM dst;
    TRUNCATE TABLE dst;
"

echo "deduplicate_insert_select=force_enable and insert_deduplicate=1 with stable select, deduplication happens"
${CLICKHOUSE_CLIENT} --deduplicate_insert_select=force_enable -q "
    INSERT INTO dst SELECT 1, 'one' ORDER BY all;
    INSERT INTO dst SELECT 1, 'one' ORDER BY all;
    SELECT count() FROM dst;
    TRUNCATE TABLE dst;
"

echo "deduplicate_insert_select=enable_even_for_bad_queries and insert_deduplicate=1 with non-stable select, deduplication happens"
${CLICKHOUSE_CLIENT} --deduplicate_insert_select='enable_when_possible' -q "
    INSERT INTO dst SELECT number AS id, toString(number) AS data FROM numbers(10);
    INSERT INTO dst SELECT number AS id, toString(number) AS data FROM numbers(10);
    SELECT count() FROM dst;
    TRUNCATE TABLE dst;
"
