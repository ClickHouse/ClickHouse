#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.mergetree;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.distributed;"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.mergetree (a Int64, b Int64, c Int64) ENGINE = MergeTree ORDER BY (a, b);"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.distributed AS test.mergetree ENGINE = Distributed(test_unavailable_shard, test, mergetree, jumpConsistentHash(a+b, 2));"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test.mergetree VALUES (0, 0, 0);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.mergetree VALUES (1, 0, 0);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.mergetree VALUES (0, 1, 1);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.mergetree VALUES (1, 1, 1);"

# Should fail because second shard is unavailable
${CLICKHOUSE_CLIENT} --query "SELECT count(*) FROM test.distributed;" 2>&1 \
| fgrep -q "All connection tries failed" && echo 'OK' || echo 'FAIL'

# Should fail without setting `optimize_skip_unused_shards`
${CLICKHOUSE_CLIENT} --query "SELECT count(*) FROM test.distributed WHERE a = 0 AND b = 0;" 2>&1 \
| fgrep -q "All connection tries failed" && echo 'OK' || echo 'FAIL'

# Should pass now
${CLICKHOUSE_CLIENT} -n --query="
    SET optimize_skip_unused_shards = 1;
    SELECT count(*) FROM test.distributed WHERE a = 0 AND b = 0;
"

# Should still fail because of matching unavailable shard
${CLICKHOUSE_CLIENT} -n --query="
    SET optimize_skip_unused_shards = 1;
    SELECT count(*) FROM test.distributed WHERE a = 2 AND b = 2;
" 2>&1 \ | fgrep -q "All connection tries failed" && echo 'OK' || echo 'FAIL'

# Try more complext expressions for constant folding - all should pass.

${CLICKHOUSE_CLIENT} -n --query="
    SET optimize_skip_unused_shards = 1;
    SELECT count(*) FROM test.distributed WHERE a = 1 AND a = 0 AND b = 0;
"

${CLICKHOUSE_CLIENT} -n --query="
    SET optimize_skip_unused_shards = 1;
    SELECT count(*) FROM test.distributed WHERE a IN (0, 1) AND b IN (0, 1);
"

${CLICKHOUSE_CLIENT} -n --query="
    SET optimize_skip_unused_shards = 1;
    SELECT count(*) FROM test.distributed WHERE a = 0 AND b = 0 OR a = 1 AND b = 1;
"

# TODO: should pass one day.
#${CLICKHOUSE_CLIENT} -n --query="
#    SET optimize_skip_unused_shards = 1;
#    SELECT count(*) FROM test.distributed WHERE a = 0 AND b >= 0 AND b <= 1;
#"

${CLICKHOUSE_CLIENT} -n --query="
    SET optimize_skip_unused_shards = 1;
    SELECT count(*) FROM test.distributed WHERE a = 0 AND b = 0 AND c = 0;
"

${CLICKHOUSE_CLIENT} -n --query="
    SET optimize_skip_unused_shards = 1;
    SELECT count(*) FROM test.distributed WHERE a = 0 AND b = 0 AND c != 10;
"

${CLICKHOUSE_CLIENT} -n --query="
    SET optimize_skip_unused_shards = 1;
    SELECT count(*) FROM test.distributed WHERE a = 0 AND b = 0 AND (a+b)*b != 12;
"

${CLICKHOUSE_CLIENT} -n --query="
    SET optimize_skip_unused_shards = 1;
    SELECT count(*) FROM test.distributed WHERE (a = 0 OR a = 1) AND (b = 0 OR b = 1);
"

# These ones should fail.

${CLICKHOUSE_CLIENT} -n --query="
    SET optimize_skip_unused_shards = 1;
    SELECT count(*) FROM test.distributed WHERE a = 0 AND b <= 1;
" 2>&1 \ | fgrep -q "All connection tries failed" && echo 'OK' || echo 'FAIL'

${CLICKHOUSE_CLIENT} -n --query="
    SET optimize_skip_unused_shards = 1;
    SELECT count(*) FROM test.distributed WHERE a = 0 AND c = 0;
" 2>&1 \ | fgrep -q "All connection tries failed" && echo 'OK' || echo 'FAIL'

${CLICKHOUSE_CLIENT} -n --query="
    SET optimize_skip_unused_shards = 1;
    SELECT count(*) FROM test.distributed WHERE a = 0 OR a = 1 AND b = 0;
" 2>&1 \ | fgrep -q "All connection tries failed" && echo 'OK' || echo 'FAIL'

${CLICKHOUSE_CLIENT} -n --query="
    SET optimize_skip_unused_shards = 1;
    SELECT count(*) FROM test.distributed WHERE a = 0 AND b = 0 OR a = 2 AND b = 2;
" 2>&1 \ | fgrep -q "All connection tries failed" && echo 'OK' || echo 'FAIL'

${CLICKHOUSE_CLIENT} -n --query="
    SET optimize_skip_unused_shards = 1;
    SELECT count(*) FROM test.distributed WHERE a = 0 AND b = 0 OR c = 0;
" 2>&1 \ | fgrep -q "All connection tries failed" && echo 'OK' || echo 'FAIL'
