#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A materialized CTE hidden in a SQL UDF body reaches a stored view definition only after UDF expansion, so the
# definition-time guard must see the expanded definition. The function name is per-database: UDF names are
# server-global and the flaky check runs this test concurrently with itself.

F="${CLICKHOUSE_DATABASE}_f"
GUARD_ON="--enable_analyzer 1 --enable_materialized_cte 1 --force_materialized_cte 1"
GUARD_OFF="--enable_analyzer 1 --enable_materialized_cte 1 --force_materialized_cte 0"

${CLICKHOUSE_CLIENT} -q "CREATE FUNCTION $F AS () -> (WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() FROM c AS a, c AS b)"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE src (x UInt64) ENGINE = Memory;
    INSERT INTO src SELECT number FROM numbers(3);
    CREATE TABLE dst (n UInt64) ENGINE = Memory;
    CREATE MATERIALIZED VIEW mv TO dst AS SELECT count() AS n FROM src;
"

echo "-- guard on: a view or MODIFY QUERY whose UDF hides a materialized CTE is rejected"
${CLICKHOUSE_CLIENT} ${GUARD_ON} -q "CREATE VIEW v AS SELECT $F() AS n" 2>&1 | grep -oF 'SUPPORT_IS_DISABLED' | head -n 1
${CLICKHOUSE_CLIENT} ${GUARD_ON} -q "ALTER TABLE mv MODIFY QUERY SELECT $F() AS n" 2>&1 | grep -oF 'SUPPORT_IS_DISABLED' | head -n 1

echo "-- guard off: accepted and inlined"
${CLICKHOUSE_CLIENT} ${GUARD_OFF} -q "CREATE VIEW v AS SELECT $F() AS n"
${CLICKHOUSE_CLIENT} ${GUARD_OFF} -q "SELECT * FROM v"
${CLICKHOUSE_CLIENT} ${GUARD_OFF} -q "ALTER TABLE mv MODIFY QUERY SELECT $F() AS n"

${CLICKHOUSE_CLIENT} -q "DROP VIEW v; DROP TABLE mv; DROP TABLE dst; DROP TABLE src; DROP FUNCTION $F"
