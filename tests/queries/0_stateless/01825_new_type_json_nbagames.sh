#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS nbagames"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS nbagames_string"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS nbagames_from_string"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE nbagames (data JSON) ENGINE = MergeTree ORDER BY tuple()" --allow_experimental_json_type 1

cat $CUR_DIR/data_json/nbagames_sample.json | ${CLICKHOUSE_CLIENT} -q "INSERT INTO nbagames FORMAT JSONAsObject"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM nbagames WHERE NOT ignore(*)"
${CLICKHOUSE_CLIENT} -q "SELECT DISTINCT arrayJoin(JSONAllPathsWithTypes(data)) as path from nbagames order by path"
${CLICKHOUSE_CLIENT} -q "SELECT DISTINCT arrayJoin(JSONAllPathsWithTypes(arrayJoin(data.teams[]))) as path from nbagames order by path"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q  \
    "SELECT teams.name.:String AS name, sum(teams.won.:Int64) AS wins FROM nbagames \
    ARRAY JOIN data.teams[] AS teams GROUP BY name \
    ORDER BY wins DESC LIMIT 5;"

${CLICKHOUSE_CLIENT} -q "SELECT DISTINCT arrayJoin(JSONAllPathsWithTypes(arrayJoin(arrayJoin(data.teams[].players[])))) as path from nbagames order by path"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 -q \
"SELECT player, sum(triple_double) AS triple_doubles FROM \
( \
    SELECT \
        arrayJoin(arrayJoin(data.teams[].players[])) as players, \
        players.player.:String as player, \
        ((players.pts.:Int64 >= 10) + \
        (players.ast.:Int64 >= 10) + \
        (players.blk.:Int64 >= 10) + \
        (players.stl.:Int64 >= 10) + \
        (players.trb.:Int64 >= 10)) >= 3 AS triple_double \
        from nbagames \
) \
GROUP BY player ORDER BY triple_doubles DESC, player LIMIT 5"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE nbagames_string (data String) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE nbagames_from_string (data JSON) ENGINE = MergeTree ORDER BY tuple()" --allow_experimental_json_type 1

cat $CUR_DIR/data_json/nbagames_sample.json | ${CLICKHOUSE_CLIENT} -q "INSERT INTO nbagames_string FORMAT JSONAsString"
${CLICKHOUSE_CLIENT} -q "INSERT INTO nbagames_from_string SELECT data FROM nbagames_string"

${CLICKHOUSE_CLIENT} -q "SELECT \
    (SELECT groupUniqArrayMap(JSONAllPathsWithTypes(data)), sum(cityHash64(toString(data))) FROM nbagames_from_string) = \
    (SELECT groupUniqArrayMap(JSONAllPathsWithTypes(data)), sum(cityHash64(toString(data))) FROM nbagames)"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS nbagames"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS nbagames_string"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS nbagames_from_string"
