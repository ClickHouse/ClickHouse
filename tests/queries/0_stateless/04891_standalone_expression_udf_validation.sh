#!/usr/bin/env bash

# Tags: no-parallel
# - no-parallel -- SQL UDFs are global server objects; the flaky check runs the same test concurrently and the CREATE FUNCTION statements would collide.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiquery <<'SQL'
CREATE TABLE t_04891 (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE s_04891 (a UInt64) ENGINE = Memory;
CREATE FUNCTION f_04891 AS x -> x IN s_04891;
CREATE FUNCTION g_04891 AS x -> COLUMNS('^a$');
SQL

${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_04891 ADD CONSTRAINT c CHECK f_04891(a)" 2>&1 | grep -m1 -o 'BAD_ARGUMENTS'
${CLICKHOUSE_CLIENT} -q "CREATE HYPOTHETICAL INDEX i_04891 ON t_04891 (f_04891(a)) TYPE minmax GRANULARITY 1" 2>&1 | grep -m1 -o 'BAD_ARGUMENTS'
${CLICKHOUSE_CLIENT} -q "CREATE HYPOTHETICAL INDEX i_04891 ON t_04891 (g_04891(a)) TYPE minmax GRANULARITY 1" 2>&1 | grep -m1 -o 'BAD_ARGUMENTS'

${CLICKHOUSE_CLIENT} --multiquery <<'SQL'
DROP FUNCTION f_04891;
DROP FUNCTION g_04891;
DROP TABLE s_04891;
DROP TABLE t_04891;
SQL
