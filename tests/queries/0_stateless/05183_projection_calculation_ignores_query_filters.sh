#!/usr/bin/env bash

# A projection is calculated over the data being written. The source of the calculation borrows the
# name of `system.one`, so a row policy or an `additional_table_filters` entry for that name must not
# reshape the data of the projection part. Otherwise wrong aggregates are persisted in the projection,
# and they stay wrong after the policy is dropped, because a merge combines the stored states instead
# of recomputing them from the source data.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05183_${CLICKHOUSE_DATABASE}"
policy="policy_05183_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --multiquery "
DROP ROW POLICY IF EXISTS ${policy} ON system.one;
DROP USER IF EXISTS ${user};
CREATE USER ${user} NOT IDENTIFIED;
GRANT CREATE TABLE, DROP TABLE, INSERT, SELECT, OPTIMIZE ON ${CLICKHOUSE_DATABASE}.* TO ${user};
"

function create_and_fill()
{
    local settings="$1"
    $CLICKHOUSE_CLIENT --multiquery "
    DROP TABLE IF EXISTS t_projection_filters;
    CREATE TABLE t_projection_filters (a UInt64, b String, PROJECTION p (SELECT b, sum(a), count() GROUP BY b))
    ENGINE = MergeTree ORDER BY a SETTINGS materialize_projections_on_insert = 1;
    INSERT INTO t_projection_filters SELECT number % 3, toString(number % 2) FROM numbers(10) ${settings};
    "
}

function check()
{
    # The projection must give the same answer as the table itself.
    $CLICKHOUSE_CLIENT --multiquery "
    SELECT b, sum(a), count() FROM t_projection_filters GROUP BY b ORDER BY b
        SETTINGS optimize_use_projections = 0;
    SELECT b, sum(a), count() FROM t_projection_filters GROUP BY b ORDER BY b
        SETTINGS optimize_aggregation_in_order = 0, parallel_replicas_local_plan = 1,
                 parallel_replicas_support_projection = 1, force_optimize_projection = 1;
    DROP TABLE t_projection_filters;
    "
}

echo 'a row policy on system.one'
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY ${policy} ON system.one USING a != 0 TO ${user}"
$CLICKHOUSE_CLIENT --user "${user}" --multiquery "
DROP TABLE IF EXISTS t_projection_filters;
CREATE TABLE t_projection_filters (a UInt64, b String, PROJECTION p (SELECT b, sum(a), count() GROUP BY b))
ENGINE = MergeTree ORDER BY a SETTINGS materialize_projections_on_insert = 1;
INSERT INTO t_projection_filters SELECT number % 3, toString(number % 2) FROM numbers(10);
"
check
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY ${policy} ON system.one"

echo 'additional_table_filters for system.one'
create_and_fill "SETTINGS additional_table_filters = {'system.one' : 'a != 0'}"
check

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
