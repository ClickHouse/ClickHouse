#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A skip index over an `ALIAS` column that hides an `IN` over a table is rejected on `CREATE TABLE`,
# but metadata created before that validation existed must keep loading. `CREATE TABLE new AS old`
# (and `CLONE AS`) copies the source table's skip indices into fresh metadata, so the same validation
# must run there too: a grandfathered forbidden index must not be copyable into a new table.
#
# To obtain a grandfathered table, create it with a valid alias in a `clickhouse local` session with
# a persistent path, rewrite the alias in the stored metadata file to a forbidden one, and start a
# second session.

WORK_DIR=$CLICKHOUSE_TMP/05320_create_as_grandfathered_skip_index
rm -rf "$WORK_DIR"

$CLICKHOUSE_LOCAL --path "$WORK_DIR" < /dev/null --query "
    CREATE TABLE t_set (id UInt64) ENGINE = Set;
    INSERT INTO t_set VALUES (1);
    CREATE TABLE src (x UInt64, y UInt64, a ALIAS x + y, INDEX idx a TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src (x, y) VALUES (1, 2);
"

sed -i "s/ALIAS x + y/ALIAS x IN default.t_set/" "$WORK_DIR"/store/*/*/src.sql

$CLICKHOUSE_LOCAL --path "$WORK_DIR" < /dev/null --query "
    -- The grandfathered table itself keeps loading and is readable.
    SELECT 'load-ok', count() FROM src;

    -- But its forbidden index must not be copied into fresh metadata.
    CREATE TABLE dst AS src; -- { serverError BAD_ARGUMENTS }
    CREATE TABLE dst CLONE AS src; -- { serverError BAD_ARGUMENTS }
    SELECT 'copy-rejected', count() FROM system.tables WHERE database = currentDatabase() AND name = 'dst';

    -- An ordinary skip index over an \`ALIAS\` column is still copyable.
    CREATE TABLE ok (x UInt64, y UInt64, a ALIAS x + y, INDEX idx a TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE ok2 AS ok;
    INSERT INTO ok2 (x, y) VALUES (1, 2);
    SELECT 'allowed-copy-ok', count() FROM ok2 WHERE a = 3;
"

rm -rf "$WORK_DIR"
