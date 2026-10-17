#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: polyglot requires Rust build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A polyglot `INSERT ... VALUES` reads its inline data from the transpiled query, which the server
# owns. Consumers of a copied context (`Context::createCopy`) - the asynchronous insert queue and
# the `Distributed` sink - must still see that transpiled query.

POLY="--allow_experimental_polyglot_dialect 1 --dialect polyglot --polyglot_dialect postgresql"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t (x Int32) ENGINE = MergeTree ORDER BY x"

$CLICKHOUSE_CLIENT $POLY --async_insert 1 --wait_for_async_insert 1 -q "INSERT INTO t VALUES (1), (2), (3)"
$CLICKHOUSE_CLIENT $POLY --async_insert 1 --wait_for_async_insert 1 -q "INSERT INTO t VALUES (4)"
echo "--- async insert (expect: 10 4) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"

$CLICKHOUSE_CLIENT -q "CREATE TABLE d (x Int32) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t)"
$CLICKHOUSE_CLIENT $POLY --distributed_foreground_insert 1 -q "INSERT INTO d VALUES (5), (6)"
echo "--- distributed insert (expect: 21 6) ---"
$CLICKHOUSE_CLIENT -q "SELECT sum(x), count() FROM t"
