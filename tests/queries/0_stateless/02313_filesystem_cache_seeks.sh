#!/usr/bin/env bash
# Tags: long, no-fasttest, no-object-storage, no-random-settings, no-parallel:filesystem-cache
# Tag no-parallel: clears the shared filesystem caches of the storage policies below and then
# repeatedly reads through them.

# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


client_opts=(
  --distributed_ddl_output_mode  'null_status_on_timeout'
)

# Caches are registered under their disk names, which differ from the policy names for
# `s3_cache_multi` (a cache over a cache over `s3_cache`) and `azure_cache`. Clear only these: an
# unnamed `SYSTEM CLEAR FILESYSTEM CACHE` also wipes the per-test cache disks of concurrent tests.
declare -A POLICY_CACHES=(
    ['s3_cache']="'s3_cache'"
    ['local_cache']="'local_cache'"
    ['s3_cache_multi']="'s3_cache_multi_2' 's3_cache_multi' 's3_cache'"
    ['azure_cache']="'cached_azure'"
)

for STORAGE_POLICY in 's3_cache' 'local_cache' 's3_cache_multi' 'azure_cache'; do
    echo "Using storage policy: $STORAGE_POLICY"
    for CACHE in ${POLICY_CACHES[$STORAGE_POLICY]}; do
        $CLICKHOUSE_CLIENT --query "SYSTEM CLEAR FILESYSTEM CACHE $CACHE"
    done

    $CLICKHOUSE_CLIENT "${client_opts[@]}" --query "DROP TABLE IF EXISTS test_02313" > /dev/null

    # `s3_cache_multi` storage policy is incompatible with types that use multiple streams.
    # To ensure compatibility, force `serialization_info_version` to `default` in this case.
    if [ "$STORAGE_POLICY" = "s3_cache_multi" ]; then
        STRING_SERIALIZE_SETTING=", serialization_info_version = 'basic'"
    else
        STRING_SERIALIZE_SETTING=""
    fi

    $CLICKHOUSE_CLIENT "${client_opts[@]}" --query "CREATE TABLE test_02313 (id Int32, val String)
    ENGINE = MergeTree()
    ORDER BY tuple()
    SETTINGS storage_policy = '$STORAGE_POLICY' $STRING_SERIALIZE_SETTING" > /dev/null

    $CLICKHOUSE_CLIENT --enable_filesystem_cache_on_write_operations=0 --query "INSERT INTO test_02313
    SELECT * FROM
        generateRandom('id Int32, val String')
    LIMIT 100000"

    $CLICKHOUSE_CLIENT --query "SELECT * FROM test_02313 WHERE val LIKE concat('%', randomPrintableASCII(3), '%') FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SELECT * FROM test_02313 WHERE val LIKE concat('%', randomPrintableASCII(3), '%') FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SELECT * FROM test_02313 WHERE val LIKE concat('%', randomPrintableASCII(3), '%') FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SELECT * FROM test_02313 WHERE val LIKE concat('%', randomPrintableASCII(3), '%') FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SELECT * FROM test_02313 WHERE val LIKE concat('%', randomPrintableASCII(3), '%') FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SELECT * FROM test_02313 WHERE val LIKE concat('%', randomPrintableASCII(3), '%') FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SELECT * FROM test_02313 WHERE val LIKE concat('%', randomPrintableASCII(3), '%') FORMAT Null"

    $CLICKHOUSE_CLIENT "${client_opts[@]}" --query "DROP TABLE test_02313" > /dev/null

done
