#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS nbagames"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE nbagames (data JSON) ENGINE = MergeTree ORDER BY tuple()" --allow_experimental_object_type 1

cat $CUR_DIR/data_json/nbagames_sample.json | ${CLICKHOUSE_CLIENT} -q "INSERT INTO nbagames FORMAT JSONAsObject"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM nbagames WHERE NOT ignore(*)"
${CLICKHOUSE_CLIENT} -q "DESC nbagames SETTINGS describe_extend_object_types = 1"

${CLICKHOUSE_CLIENT} -q  \
    "SELECT teams.name AS name, sum(teams.won) AS wins FROM nbagames \
    ARRAY JOIN data.teams AS teams GROUP BY name \
    ORDER BY wins DESC LIMIT 5;"

${CLICKHOUSE_CLIENT} -q \
"SELECT player, sum(triple_double) AS triple_doubles FROM \
( \
    SELECT \
        tupleElement(players, 'player') AS player, \
        ((tupleElement(players, 'pts') >= 10) + \
        (tupleElement(players, 'ast') >= 10) + \
        (tupleElement(players, 'blk') >= 10) + \
        (tupleElement(players, 'stl') >= 10) + \
        (tupleElement(players, 'trb') >= 10)) >= 3 AS triple_double \
    FROM \
    ( \
        SELECT arrayJoin(arrayJoin(data.teams.players)) as players from nbagames \
    ) \
) \
GROUP BY player ORDER BY triple_doubles DESC, player LIMIT 5"


${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS nbagames"
