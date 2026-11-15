#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database, long, no-debug, no-asan, no-tsan, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Create many tables in the database
NUM_TABLES=1000
NUM_COLUMNS=1000
THREADS=$(nproc)

COLUMNS=$(seq 1 $NUM_COLUMNS | sed -r -e 's/(.+)/c\1 UInt8, /' | tr -d '\n')

seq 1 $NUM_TABLES | xargs -P "${THREADS}" -I{} bash -c "
    echo -n '.'
    $CLICKHOUSE_CLIENT --query 'CREATE OR REPLACE TABLE test{} (${COLUMNS} end String) ENGINE = Memory'
"
echo

$CLICKHOUSE_CLIENT "
DROP USER IF EXISTS test_03147;
CREATE USER test_03147;
GRANT SELECT (end) ON ${CLICKHOUSE_DATABASE}.test1 TO test_03147;
"

# This query was slow in previous ClickHouse versions for several reasons:
# - tables and databases without SHOW TABLES access were still checked for SHOW COLUMNS access for every column in every table;
# - excessive logging of "access granted" and "access denied"

# The test could succeed even on the previous version, but it will show up as being too slow.
$CLICKHOUSE_CLIENT --user test_03147 --query "SELECT name FROM system.columns WHERE database = currentDatabase()"

$CLICKHOUSE_CLIENT "
DROP USER test_03147;
"
