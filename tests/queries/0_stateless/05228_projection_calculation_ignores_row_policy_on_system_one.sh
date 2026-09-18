#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: a row policy on `system.one` is a server-wide side effect. While it exists, every other
# user is denied a `FROM`-less `SELECT` (`throw_on_unmatched_row_policies` is enabled in the test
# config), so concurrently running tests would fail with `ACCESS_DENIED`.

# A projection is calculated over the data being written. The source of the calculation borrows the
# name of `system.one`, so a row policy for that name must not reshape the data of the projection
# part. Otherwise wrong aggregates are persisted in the projection, and they stay wrong after the
# policy is dropped, because a merge combines the stored states instead of recomputing them from the
# source data.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05228_${CLICKHOUSE_DATABASE}"
policy="policy_05228_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --multiquery "
DROP ROW POLICY IF EXISTS ${policy} ON system.one;
DROP USER IF EXISTS ${user};
CREATE USER ${user} NOT IDENTIFIED;
GRANT CREATE TABLE, DROP TABLE, INSERT, SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${user};
GRANT TABLE ENGINE ON MergeTree TO ${user};
CREATE ROW POLICY ${policy} ON system.one USING a != 0 TO ${user};
"

$CLICKHOUSE_CLIENT --user "${user}" --multiquery "
DROP TABLE IF EXISTS t_projection_row_policy;
CREATE TABLE t_projection_row_policy (a UInt64, b String, PROJECTION p (SELECT b, sum(a), count() GROUP BY b))
ENGINE = MergeTree ORDER BY a SETTINGS materialize_projections_on_insert = 1;
INSERT INTO t_projection_row_policy SELECT number % 3, toString(number % 2) FROM numbers(10);
"

# The policy is dropped before the check, so the check itself is not affected by it.
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY ${policy} ON system.one"

# The projection must give the same answer as the table itself.
$CLICKHOUSE_CLIENT --multiquery "
SELECT b, sum(a), count() FROM t_projection_row_policy GROUP BY b ORDER BY b
    SETTINGS optimize_use_projections = 0;
SELECT b, sum(a), count() FROM t_projection_row_policy GROUP BY b ORDER BY b
    SETTINGS optimize_aggregation_in_order = 0, parallel_replicas_local_plan = 1,
             parallel_replicas_support_projection = 1, force_optimize_projection = 1;
DROP TABLE t_projection_row_policy;
DROP USER ${user};
"
