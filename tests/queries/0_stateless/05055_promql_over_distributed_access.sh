#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A user without the SELECT grant on a Distributed table must not learn anything about it from prometheusQuery,
# not even from DESCRIBE or CREATE TABLE AS, which inspect the table before it is read.

user="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table 1 --query "
DROP USER IF EXISTS ${user};
CREATE USER ${user} NOT IDENTIFIED;
GRANT CREATE TEMPORARY TABLE, CREATE TABLE ON *.* TO ${user};
CREATE TABLE ts_local ENGINE = TimeSeries;
CREATE TABLE ts_dist AS ts_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), ts_local);
CREATE TABLE mt (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO ts_local (metric_name, tags, samples) VALUES ('m', map('job', 'a'), [(toDateTime64(140, 3), 1)]);
"

function run()
{
    ${CLICKHOUSE_CLIENT} --user "${user}" --query "$1" 2>&1 | grep -o -m1 -E 'ACCESS_DENIED|UNEXPECTED_TABLE_ENGINE|MergeTree' | paste -sd ' ' - | grep . || echo 'OK'
}

function all()
{
    echo "describe: $(run "DESCRIBE TABLE prometheusQuery(${CLICKHOUSE_DATABASE}.ts_dist, 'm', 140)")"
    echo "describe range: $(run "DESCRIBE TABLE prometheusQueryRange(${CLICKHOUSE_DATABASE}.ts_dist, 'm', 100, 140, 10)")"
    echo "create as: $(run "CREATE TABLE ${CLICKHOUSE_DATABASE}.stored AS prometheusQuery(${CLICKHOUSE_DATABASE}.ts_dist, 'm', 140)")"
    echo "select: $(run "SELECT count() FROM prometheusQuery(${CLICKHOUSE_DATABASE}.ts_dist, 'm', 140) FORMAT Null")"
    echo "other engine: $(run "DESCRIBE TABLE prometheusQuery(${CLICKHOUSE_DATABASE}.mt, 'm', 140)")"
}

echo '--- no grants'
all

echo '--- with grants'
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${user}; GRANT READ ON REMOTE TO ${user}"
all

${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
