#!/usr/bin/env bash
# Tags: no-fasttest, long, no-object-storage
# ^ no-object-storage: too slow

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ghdata"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ghdata (data Object('json')) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi'" --allow_experimental_object_type

cat $CUR_DIR/data_json/ghdata_sample.json | ${CLICKHOUSE_CLIENT} \
  --max_memory_usage 10G --query "INSERT INTO ghdata FORMAT JSONAsObject"

${CLICKHOUSE_CLIENT} -q "ALTER TABLE ghdata MODIFY column data JSON SETTINGS mutations_sync=1" --enable_json_type 1

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ghdata WHERE NOT ignore(*)"

${CLICKHOUSE_CLIENT} -q \
"SELECT data.repo.name, count() AS stars FROM ghdata \
    WHERE data.type = 'WatchEvent' GROUP BY data.repo.name ORDER BY stars DESC, data.repo.name LIMIT 5" --allow_suspicious_types_in_group_by=1 --allow_suspicious_types_in_order_by=1

${CLICKHOUSE_CLIENT} --enable_analyzer=1 -q \
"SELECT data.payload.commits[].author.name AS name, count() AS c FROM ghdata \
    ARRAY JOIN data.payload.commits[].author.name \
    GROUP BY name ORDER BY c DESC, name LIMIT 5" --allow_suspicious_types_in_group_by=1 --allow_suspicious_types_in_order_by=1

${CLICKHOUSE_CLIENT} -q "SELECT max(data.payload.pull_request.assignees[].size0) FROM ghdata"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ghdata"
