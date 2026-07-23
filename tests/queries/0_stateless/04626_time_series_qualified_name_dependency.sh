#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# ^ no-fasttest: TimeSeries tables are experimental and follow the other TimeSeries tests;
#   they are not supported in Replicated databases.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A database-qualified `timeSeries*` target inside a stored query names a stable object, so it registers a
# referential dependency, and with `check_referential_table_dependencies = 1` the target table cannot be
# dropped from under the view. This lives in a shell test because the database name must be a literal in the
# persisted view body: a `{...:Identifier}` parameter is persisted unsubstituted and would not name a stable
# object, and a fixed name would collide between concurrent runs of this test (e.g. in the flaky check).
# The name is derived from `$CLICKHOUSE_DATABASE`, which is unique per run.

db="${CLICKHOUSE_DATABASE}_dep"

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 --query "
    DROP DATABASE IF EXISTS $db;
    CREATE DATABASE $db;
    CREATE TABLE $db.ts_dep ENGINE = TimeSeries;
"

# Over `cluster('test_shard_localhost', ...)` the function reads its target on the local replica, and the
# database-qualified target is a referential dependency of the view: the DROP of the target is rejected.
# (Contrast with the `remote()` case in 04538: there the dependency visitor treats the target as remote-only
# and records nothing.)
$CLICKHOUSE_CLIENT --query "CREATE VIEW $db.v_ts_cluster AS SELECT * FROM cluster('test_shard_localhost', timeSeriesData($db.ts_dep));"
$CLICKHOUSE_CLIENT --check_referential_table_dependencies=1 --query "DROP TABLE $db.ts_dep;" 2>&1 | grep -o "HAVE_DEPENDENT_OBJECTS" | head -1
$CLICKHOUSE_CLIENT --query "DROP VIEW $db.v_ts_cluster;"

# The same holds for a direct (non-cluster) qualified `timeSeries*` call in a view body. An unqualified
# spelling would register nothing (it is resolved by the querying session at execution time; see 04625).
$CLICKHOUSE_CLIENT --query "CREATE VIEW $db.v_ts_direct AS SELECT * FROM timeSeriesMetrics($db.ts_dep);"
$CLICKHOUSE_CLIENT --check_referential_table_dependencies=1 --query "DROP TABLE $db.ts_dep;" 2>&1 | grep -o "HAVE_DEPENDENT_OBJECTS" | head -1
$CLICKHOUSE_CLIENT --query "DROP VIEW $db.v_ts_direct;"

$CLICKHOUSE_CLIENT --query "DROP DATABASE $db;"
