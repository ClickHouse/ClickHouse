#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The SQLite counterpart of 03251_insert_sparse_all_formats, which excludes the SQLite format because
# its serial loop over every I/O format already sits at the per-test timeout on debug builds. Inserting
# through the format with enable_parsing_to_custom_serialization must round-trip sparse columns.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_sparse_sqlite;
    CREATE TABLE t_sparse_sqlite (a UInt64, b UInt64, c String) ENGINE = MergeTree ORDER BY a;
"

$CLICKHOUSE_CLIENT --query "INSERT INTO t_sparse_sqlite(a) SELECT number FROM numbers(1000)"

$CLICKHOUSE_CLIENT --query "SELECT number AS a, 0::UInt64 AS b, '' AS c FROM numbers(1000) FORMAT SQLite" \
    | $CLICKHOUSE_CLIENT --enable_parsing_to_custom_serialization=1 --query "INSERT INTO t_sparse_sqlite FORMAT SQLite"

$CLICKHOUSE_CLIENT --query "SELECT number AS a FROM numbers(1000) FORMAT SQLite" \
    | $CLICKHOUSE_CLIENT --enable_parsing_to_custom_serialization=1 --query "INSERT INTO t_sparse_sqlite(a) FORMAT SQLite"

$CLICKHOUSE_CLIENT --query "SELECT sum(sipHash64(*)) FROM t_sparse_sqlite"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_sparse_sqlite"
