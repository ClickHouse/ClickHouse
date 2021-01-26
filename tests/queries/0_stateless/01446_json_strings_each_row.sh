#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

echo "DROP TABLE IF EXISTS test_table;" | ${CLICKHOUSE_CLIENT}
echo "DROP TABLE IF EXISTS test_table_2;" | ${CLICKHOUSE_CLIENT}
echo "SELECT 1;" | ${CLICKHOUSE_CLIENT}
# Check JSONStringsEachRow Output
echo "CREATE TABLE test_table (value UInt8, name String) ENGINE = MergeTree() ORDER BY value;" | ${CLICKHOUSE_CLIENT}
echo "INSERT INTO test_table VALUES (1, 'a'), (2, 'b'), (3, 'c');" | ${CLICKHOUSE_CLIENT}
echo "SELECT * FROM test_table FORMAT JSONStringsEachRow;" | ${CLICKHOUSE_CLIENT}
echo "SELECT 2;" | ${CLICKHOUSE_CLIENT}
# Check Totals
echo "SELECT name, count() AS c FROM test_table GROUP BY name WITH TOTALS ORDER BY name FORMAT JSONStringsEachRow;" | ${CLICKHOUSE_CLIENT}
echo "SELECT 3;" | ${CLICKHOUSE_CLIENT}
# Check JSONStringsEachRowWithProgress Output
echo "SELECT 1 as a FROM system.one FORMAT JSONStringsEachRowWithProgress;" | ${CLICKHOUSE_CLIENT} | grep -v progress
echo "SELECT 4;" | ${CLICKHOUSE_CLIENT}
# Check Totals
echo "SELECT 1 as a FROM system.one GROUP BY a WITH TOTALS ORDER BY a FORMAT JSONStringsEachRowWithProgress;" | ${CLICKHOUSE_CLIENT} | grep -v progress
echo "DROP TABLE IF EXISTS test_table;" | ${CLICKHOUSE_CLIENT}
echo "SELECT 5;" | ${CLICKHOUSE_CLIENT}
# Check JSONStringsEachRow Input
echo "CREATE TABLE test_table (v1 String, v2 UInt8, v3 DEFAULT v2 * 16, v4 UInt8 DEFAULT 8) ENGINE = MergeTree() ORDER BY v2;" | ${CLICKHOUSE_CLIENT}
echo 'INSERT INTO test_table FORMAT JSONStringsEachRow {"v1": "first", "v2": "1", "v3": "2", "v4": "NULL"} {"v1": "second", "v2": "2", "v3": "null", "v4": "6"};' | ${CLICKHOUSE_CLIENT}
echo "SELECT * FROM test_table FORMAT JSONStringsEachRow;" | ${CLICKHOUSE_CLIENT}
echo "TRUNCATE TABLE test_table;" | ${CLICKHOUSE_CLIENT}
echo "SELECT 6;" | ${CLICKHOUSE_CLIENT}
# Check input_format_null_as_default = 1
echo 'INSERT INTO test_table FORMAT JSONStringsEachRow {"v1": "first", "v2": "1", "v3": "2", "v4": "ᴺᵁᴸᴸ"} {"v1": "second", "v2": "2", "v3": "null", "v4": "6"};' | ${CLICKHOUSE_CLIENT} --input_format_null_as_default=1
echo "SELECT * FROM test_table FORMAT JSONStringsEachRow;" | ${CLICKHOUSE_CLIENT}
echo "TRUNCATE TABLE test_table;" | ${CLICKHOUSE_CLIENT}
echo "SELECT 7;" | ${CLICKHOUSE_CLIENT}
# Check Nested
echo "CREATE TABLE test_table_2 (v1 UInt8, n Nested(id UInt8, name String)) ENGINE = MergeTree() ORDER BY v1;" | ${CLICKHOUSE_CLIENT}
cat << END | ${CLICKHOUSE_CLIENT}
INSERT INTO test_table_2 FORMAT JSONStringsEachRow {"v1": "16", "n.id": "[15, 16, 17]", "n.name": "['first', 'second', 'third']"};
END
echo "SELECT * FROM test_table_2 FORMAT JSONStringsEachRow;" | ${CLICKHOUSE_CLIENT}
echo "TRUNCATE TABLE test_table_2;" | ${CLICKHOUSE_CLIENT}

echo "DROP TABLE IF EXISTS test_table;" | ${CLICKHOUSE_CLIENT}
echo "DROP TABLE IF EXISTS test_table_2;" | ${CLICKHOUSE_CLIENT}
