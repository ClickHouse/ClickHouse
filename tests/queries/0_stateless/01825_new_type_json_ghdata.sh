#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ghdata"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ghdata (data JSON) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi'" --allow_experimental_json_type 1

cat $CUR_DIR/data_json/ghdata_sample.json | ${CLICKHOUSE_CLIENT} -q "INSERT INTO ghdata FORMAT JSONAsObject"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ghdata WHERE NOT ignore(*)"

${CLICKHOUSE_CLIENT} -q \
"SELECT data.repo.name, count() AS stars FROM ghdata \
    WHERE data.type = 'WatchEvent' GROUP BY data.repo.name ORDER BY stars DESC, data.repo.name LIMIT 5"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q \
"SELECT data.payload.commits[].author.name AS name, count() AS c FROM ghdata \
    ARRAY JOIN data.payload.commits[].author.name \
    GROUP BY name ORDER BY c DESC, name LIMIT 5"

${CLICKHOUSE_CLIENT} -q "SELECT max(data.payload.pull_request.assignees[].size0) FROM ghdata"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ghdata"
