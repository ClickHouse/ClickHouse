#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree, no-parallel
# Tag no-fasttest: requires S3
# Tag no-shared-merge-tree: does not support replication
# Tag no-parallel: uses failpoints

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

on_exit() {
    ${CLICKHOUSE_CLIENT} -m --query "
    SYSTEM DISABLE FAILPOINT plain_object_storage_write_fail_on_directory_create;
    SYSTEM DISABLE FAILPOINT plain_object_storage_write_fail_on_directory_move;
"
}

trap on_exit EXIT

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_s3_mt_fault"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE test_s3_mt_fault (a Int32, b Int64) engine = MergeTree() ORDER BY tuple(a, b)
SETTINGS disk = disk(
    name = 03008_s3_plain_rewritable_fault,
    type = s3_plain_rewritable,
    endpoint = 'http://localhost:11111/test/03008_test_s3_mt_fault/',
    access_key_id = clickhouse,
    secret_access_key = clickhouse);
"

${CLICKHOUSE_CLIENT} --query "
INSERT INTO test_s3_mt_fault (*) VALUES (1, 2), (2, 2), (3, 1), (4, 7), (5, 10), (6, 12);
OPTIMIZE TABLE test_s3_mt_fault FINAL;
"

${CLICKHOUSE_CLIENT} --query "
SYSTEM ENABLE FAILPOINT plain_object_storage_write_fail_on_directory_create
"

${CLICKHOUSE_CLIENT} --query "
INSERT INTO test_s3_mt_fault (*) select number, number from numbers_mt(100)" 2>&1 | grep -Fq "FAULT_INJECTED"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_s3_mt_fault;"

${CLICKHOUSE_CLIENT} --query "
SYSTEM DISABLE FAILPOINT plain_object_storage_write_fail_on_directory_create;
SYSTEM ENABLE FAILPOINT plain_object_storage_write_fail_on_directory_move;
"

${CLICKHOUSE_CLIENT} --query "
INSERT INTO test_s3_mt_fault (*) select number, number from numbers_mt(100);
" 2>&1 | grep -Fq "FAULT_INJECTED"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_s3_mt_fault;"

${CLICKHOUSE_CLIENT} --query "
SYSTEM DISABLE FAILPOINT plain_object_storage_write_fail_on_directory_move;
"

# Filter out 'Removing temporary directory' because the fault injection prevents directory rename.
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_s3_mt_fault SYNC" 2>&1 | grep -v 'Removing temporary directory' ||:
