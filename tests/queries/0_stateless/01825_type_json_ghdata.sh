#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ghdata"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ghdata (data JSON) ENGINE = MergeTree ORDER BY tuple()" --allow_experimental_object_type 1

cat $CUR_DIR/data_json/ghdata_sample.json | ${CLICKHOUSE_CLIENT} -q "INSERT INTO ghdata FORMAT JSONAsObject"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ghdata WHERE NOT ignore(*)"

${CLICKHOUSE_CLIENT} -q \
"SELECT length(subcolumns.names) \
    FROM system.parts_columns \
    WHERE table = 'ghdata' AND database = '$CLICKHOUSE_DATABASE'"

${CLICKHOUSE_CLIENT} -q "WITH position(full_type, '(') AS pos
SELECT if(pos = 0, full_type, substring(full_type, 1, pos - 1)) AS type, count() AS c \
    FROM system.parts_columns ARRAY JOIN subcolumns.types AS full_type \
    WHERE table = 'ghdata' AND database = '$CLICKHOUSE_DATABASE' \
    GROUP BY type ORDER BY c DESC"

${CLICKHOUSE_CLIENT} -q \
"SELECT data.repo.name, count() AS stars FROM ghdata \
    WHERE data.type = 'WatchEvent' GROUP BY data.repo.name ORDER BY stars DESC, data.repo.name LIMIT 5"

${CLICKHOUSE_CLIENT} -q \
"SELECT data.payload.commits.author.name AS name, count() AS c FROM ghdata \
    ARRAY JOIN data.payload.commits.author.name \
    GROUP BY name ORDER BY c DESC, name LIMIT 5"

${CLICKHOUSE_CLIENT} -q "SELECT max(data.payload.pull_request.assignees.size0) FROM ghdata"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ghdata"
