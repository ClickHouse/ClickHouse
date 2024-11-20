#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

logs_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}

rm -rf ${logs_dir}

mkdir -p ${logs_dir}/

for i in {1..10}
do
	echo $i >> ${logs_dir}/a.txt
done

${CLICKHOUSE_CLIENT} --query="
DROP TABLE IF EXISTS file_log;
DROP TABLE IF EXISTS table_to_store_data;
DROP TABLE IF EXISTS file_log_mv;

CREATE TABLE file_log (
    id Int64
) ENGINE = FileLog('${logs_dir}/', 'CSV');

CREATE TABLE table_to_store_data (
    id Int64
) ENGINE = MergeTree
ORDER BY id;

CREATE MATERIALIZED VIEW file_log_mv TO table_to_store_data AS
    SELECT id
    FROM file_log
    WHERE id NOT IN (
        SELECT id
        FROM table_to_store_data
        WHERE id IN (
            SELECT id
            FROM file_log
        )
    );
"

function count()
{
	COUNT=$(${CLICKHOUSE_CLIENT} --query "select count() from table_to_store_data;")
	echo $COUNT
}

for i in {1..10}
do
	[[ $(count) -gt 0 ]] && break
	sleep 1
done

${CLICKHOUSE_CLIENT} --query "SELECT * FROM table_to_store_data ORDER BY id;"

for i in {1..20}
do
	echo $i >> ${logs_dir}/a.txt
done

for i in {1..10}
do
	[[ $(count) -gt 10 ]] && break
	sleep 1
done

${CLICKHOUSE_CLIENT} --query "SELECT * FROM table_to_store_data ORDER BY id;"

${CLICKHOUSE_CLIENT} --query="
DROP TABLE file_log;
DROP TABLE table_to_store_data;
DROP TABLE file_log_mv;
"

rm -rf ${logs_dir}
