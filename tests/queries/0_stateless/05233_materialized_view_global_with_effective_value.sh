#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A materialized view definition must not turn `enable_global_with_statement` off, and the rule looks
# at the effective value of its `SETTINGS` clause: `compatibility` (the setting was introduced in 21.2)
# and `profile` turn it off without naming it. The `profile` clause goes over HTTP: `clickhouse-client`
# applies the clause to its own session first and cannot resolve a profile there.
# https://github.com/ClickHouse/ClickHouse/issues/113711
# The settings profile is server-global, so its name carries the test database instead of a `no-parallel` tag.

PROFILE="profile_113711b_${CLICKHOUSE_DATABASE}"
MAJOR=$(${CLICKHOUSE_CLIENT} -q "SELECT concat(splitByChar('.', version())[1], '.', splitByChar('.', version())[2])")

${CLICKHOUSE_CLIENT} -nm -q "
DROP TABLE IF EXISTS mv_eff_113711b, mv_eff_compat_113711b, mv_eff_profile_113711b, src_eff_113711b, dst_eff_113711b;
CREATE TABLE src_eff_113711b (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst_eff_113711b (id UInt32) ENGINE = MergeTree ORDER BY id;
"
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS $PROFILE"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE $PROFILE SETTINGS enable_global_with_statement = 0"

DEFINITION="WITH r_eff_113711b AS MATERIALIZED (SELECT id FROM src_eff_113711b) SELECT id FROM r_eff_113711b"

echo "-- compatibility turning the setting off is rejected"
${CLICKHOUSE_CLIENT} --send_logs_level fatal -q "CREATE MATERIALIZED VIEW mv_eff_compat_113711b TO dst_eff_113711b AS $DEFINITION SETTINGS compatibility = '21.1'" 2>&1 | grep -oF 'NOT_IMPLEMENTED' | head -n 1
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'mv_eff_compat_113711b'"

echo "-- a profile turning the setting off is rejected"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "CREATE MATERIALIZED VIEW mv_eff_profile_113711b TO dst_eff_113711b AS $DEFINITION SETTINGS profile = '$PROFILE'" 2>&1 | grep -oF 'NOT_IMPLEMENTED' | head -n 1
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'mv_eff_profile_113711b'"

echo "-- the same through MODIFY QUERY"
${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW mv_eff_113711b TO dst_eff_113711b AS SELECT id FROM src_eff_113711b WHERE id > 1000"
${CLICKHOUSE_CLIENT} --send_logs_level fatal -q "ALTER TABLE mv_eff_113711b MODIFY QUERY $DEFINITION SETTINGS compatibility = '21.1'" 2>&1 | grep -oF 'NOT_IMPLEMENTED' | head -n 1
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "ALTER TABLE mv_eff_113711b MODIFY QUERY $DEFINITION SETTINGS profile = '$PROFILE'" 2>&1 | grep -oF 'NOT_IMPLEMENTED' | head -n 1
echo "-- the definition of the view is unchanged"
${CLICKHOUSE_CLIENT} -q "SELECT position(create_table_query, 'r_eff_113711b') = 0 FROM system.tables WHERE database = currentDatabase() AND name = 'mv_eff_113711b'"

echo "-- a clause that leaves the setting on is accepted"
${CLICKHOUSE_CLIENT} --enable_materialized_cte 1 -q "ALTER TABLE mv_eff_113711b MODIFY QUERY $DEFINITION SETTINGS compatibility = '$MAJOR'"
${CLICKHOUSE_CLIENT} --enable_materialized_cte 1 -q "INSERT INTO src_eff_113711b VALUES (1), (2)"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM dst_eff_113711b ORDER BY id"

${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE $PROFILE"
${CLICKHOUSE_CLIENT} -nm -q "DROP TABLE IF EXISTS mv_eff_compat_113711b, mv_eff_profile_113711b; DROP TABLE mv_eff_113711b, dst_eff_113711b, src_eff_113711b;"
