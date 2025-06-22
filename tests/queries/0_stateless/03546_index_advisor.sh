#!/usr/bin/env bash
# Tags: no-parallel
# - no-parallel - spawning bunch of processes with sanitizers can use significant amount of memory

# CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
# . "$CUR_DIR"/../shell_config.sh

rm -rf ${CLICKHOUSE_TMP}/03546_index_advisor_collection.txt

$CLICKHOUSE_CLIENT <<EOF
DROP TABLE IF EXISTS test03546;
CREATE TABLE test03546 (i Int32, s String) ORDER BY () SETTINGS index_granularity=1;
INSERT INTO test03546 VALUES (1, 'Hello, world'), (2, 'Hello, world 2'), (3, 'Goodbye, world 3'), (4, 'Goodbye, Goodbye 4');

set collection_file_path = '${CLICKHOUSE_TMP}/03546_index_advisor_collection.txt';
set max_index_advisor_pk_columns_count = 1;
set max_index_advise_index_columns_count = 4;
set index_advisor_find_best_pk = 1;
set index_advisor_find_best_index = 1;

START COLLECTING WORKLOAD;
SELECT * FROM test03546 WHERE i < 3;
SELECT * FROM test03546 WHERE i > 3;
SELECT * FROM test03546 WHERE s LIKE 'Hello, world%';
FINISH COLLECTING WORKLOAD;

DROP TABLE IF EXISTS test03546;
EOF

cat ${CLICKHOUSE_TMP}/03546_index_advisor_collection.txt

rm -rf ${CLICKHOUSE_TMP}/03546_index_advisor_collection.txt