#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ROCKSDB_DIR="/var/lib/clickhouse/store/test_rocksdb_read_only"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test_02404;" 2>&1 | grep -F -q "OK" && echo 'OK' || echo 'FAIL';
echo "----"
$CLICKHOUSE_CLIENT --query="CREATE TABLE test_02404 (key UInt64, value String) Engine=EmbeddedRocksDB(0, '${ROCKSDB_DIR}', 1) PRIMARY KEY(key);" 2>&1 | grep -F -q "OK" && echo 'OK' || echo 'FAIL';
echo "----"
$CLICKHOUSE_CLIENT --query="CREATE TABLE test_02404 (key UInt64, value String) Engine=EmbeddedRocksDB(0, '${ROCKSDB_DIR}') PRIMARY KEY(key);" 2>&1 | grep -F -q "OK" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="INSERT INTO test_02404 (key, value) VALUES (0, 'a'), (1, 'b'), (3, 'c');" 2>&1 | grep -F -q "OK" && echo 'OK' || echo 'FAIL';
echo "----"
$CLICKHOUSE_CLIENT --query="CREATE TABLE test_02404_fail (key UInt64, value String) Engine=EmbeddedRocksDB(10, '${ROCKSDB_DIR}', 1) PRIMARY KEY(key);" 2>&1 | grep -F -q "OK" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="DROP TABLE test_02404;" 2>&1 | grep -F -q "OK" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="CREATE TABLE test_02404 (key UInt64, value String) Engine=EmbeddedRocksDB(10, '${ROCKSDB_DIR}', 1) PRIMARY KEY(key);" 2>&1 | grep -F -q "OK" && echo 'OK' || echo 'FAIL';
echo "----"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test_02404;" 2>&1 | grep -F "rows in set." | awk '{print $1}';
$CLICKHOUSE_CLIENT --query="INSERT INTO test_02404 (key, value) VALUES (4, 'd');" 2>&1 | grep -F -q "OK" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="DROP TABLE test_02404;" 2>&1 | grep -F -q "OK" && echo 'OK' || echo 'FAIL';
echo "----"
rm -r ${ROCKSDB_DIR}
$CLICKHOUSE_CLIENT --query="CREATE TABLE test_02404 (key UInt64, value String) Engine=EmbeddedRocksDB(10, '${ROCKSDB_DIR}', 1) PRIMARY KEY(key);" 2>&1 | grep -F -q "OK" && echo 'OK' || echo 'FAIL';
