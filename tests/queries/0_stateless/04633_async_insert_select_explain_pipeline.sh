#!/usr/bin/env bash
# Checks that EXPLAIN PIPELINE for a user-initiated INSERT ... SELECT reflects the route the real
# query would take: the async queue transform must appear with async_insert=1 and be absent
# without it. EXPLAIN builds the interpreter directly, so it must carry the top-level provenance.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS test_async_sel_explain"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE test_async_sel_explain (id UInt32, v String)
    ENGINE = MergeTree ORDER BY id
"

# async_insert=1: the async route is eligible, so the transform is planned.
echo -n "async: "
${CLICKHOUSE_CLIENT} --async_insert=1 -q "
    EXPLAIN PIPELINE INSERT INTO test_async_sel_explain SELECT number::UInt32, toString(number) FROM numbers(1)
" | grep -q 'AsyncInsertQueueTransform' && echo "has transform" || echo "no transform"

# async_insert=0: plain synchronous insert, no async transform.
echo -n "sync: "
${CLICKHOUSE_CLIENT} --async_insert=0 -q "
    EXPLAIN PIPELINE INSERT INTO test_async_sel_explain SELECT number::UInt32, toString(number) FROM numbers(1)
" | grep -q 'AsyncInsertQueueTransform' && echo "has transform" || echo "no transform"

${CLICKHOUSE_CLIENT} -q "DROP TABLE test_async_sel_explain"
