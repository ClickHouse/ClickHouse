#!/usr/bin/env bash
# Tags: no-fasttest, use-hdfs

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "SELECT * FROM hdfs('abcd', 'CSV', 'x UInt32')" 2>&1 | grep -F -q "BAD_ARGUMENTS" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT -q "SELECT * FROM hdfs('abcd/', 'CSV', 'x UInt32')" 2>&1 | grep -F -q "BAD_ARGUMENTS" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT -q "SELECT * FROM hdfs('//abcd', 'CSV', 'x UInt32')" 2>&1 | grep -F -q "BAD_ARGUMENTS" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT -q "SELECT * FROM hdfs('//abcd/', 'CSV', 'x UInt32')" 2>&1 | grep -F -q "BAD_ARGUMENTS" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT -q "SELECT * FROM hdfs('//abcd/data', 'CSV', 'x UInt32')" 2>&1 | grep -F -q "BAD_ARGUMENTS" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT -q "SELECT * FROM hdfs('://abcd', 'CSV', 'x UInt32')" 2>&1 | grep -F -q "BAD_ARGUMENTS" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT -q "SELECT * FROM hdfs('://abcd/data', 'CSV', 'x UInt32')" 2>&1 | grep -F -q "BAD_ARGUMENTS" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT -q "SELECT * FROM hdfs('abcd:9000', 'CSV', 'x UInt32')" 2>&1 | grep -F -q "BAD_ARGUMENTS" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT -q "SELECT * FROM hdfs('abcd:9000/data', 'CSV', 'x UInt32')" 2>&1 | grep -F -q "BAD_ARGUMENTS" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT -q "SELECT * FROM hdfs('//abcd:9000/data', 'CSV', 'x UInt32')" 2>&1 | grep -F -q "BAD_ARGUMENTS" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT -q "SELECT * FROM hdfs('://abcd:9000/data', 'CSV', 'x UInt32')" 2>&1 | grep -F -q "BAD_ARGUMENTS" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT -q "SELECT * FROM hdfs('abcd/', 'CSV', 'x UInt32')" 2>&1 | grep -F -q "BAD_ARGUMENTS" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT -q "SELECT * FROM hdfs('hdfs://abcd', 'CSV', 'x UInt32')" 2>&1 | grep -F -q "BAD_ARGUMENTS" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT -q "SELECT * FROM hdfs('hdfs1:9000/data', 'CSV', 'x UInt32')" 2>&1 | grep -F -q "BAD_ARGUMENTS" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT -q "SELECT * FROM hdfs('hdfs://hdfs1/data', 'CSV', 'x UInt32')" 2>&1 | grep -F -q "NETWORK_ERROR" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT -q "SELECT * FROM hdfs('http://hdfs1:9000/data', 'CSV', 'x UInt32')" 2>&1 | grep -F -q "BAD_ARGUMENTS" && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT -q "SELECT * FROM hdfs('hdfs://hdfs1@nameservice/abcd/data', 'CSV', 'x UInt32')" 2>&1 | grep -F -q "NETWORK_ERROR" && echo 'OK' || echo 'FAIL';

