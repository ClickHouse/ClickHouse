#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `CHECK` constraint containing a subquery is rejected on `CREATE TABLE` and
# `ALTER TABLE ... ADD|MODIFY CONSTRAINT`, but metadata created before that
# validation existed must keep loading. `CREATE TABLE new AS old` (and `CLONE AS`)
# copies the source table's constraints into fresh metadata, so the same
# validation must run there too: a grandfathered forbidden constraint must not
# be copyable into a new table.
#
# To obtain a grandfathered table, create it with a valid constraint in a
# `clickhouse local` session with a persistent path, rewrite the constraint in
# the stored metadata file to a forbidden one, and start a second session.
# The subquery reads from an explicitly qualified auxiliary table (a FROM-less
# scalar like `(SELECT 1)` needs `system.one`, which does not exist yet while
# tables are being loaded at startup in `clickhouse local`).

WORK_DIR=$CLICKHOUSE_TMP/04759_create_as_grandfathered
rm -rf "$WORK_DIR"

$CLICKHOUSE_LOCAL --path "$WORK_DIR" < /dev/null --query "
    CREATE TABLE aux (c1 Int) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO aux VALUES (5);
    CREATE TABLE src (c0 Int, CONSTRAINT c CHECK c0 > 0) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO src VALUES (1);
"

sed -i "s/CHECK c0 > 0/CHECK c0 + (SELECT throwIf(max(c1) = 5) FROM default.aux) > 0/" "$WORK_DIR"/store/*/*/src.sql

$CLICKHOUSE_LOCAL --path "$WORK_DIR" < /dev/null --query "
    -- The grandfathered table itself keeps loading and is readable.
    SELECT 'load-ok', count() FROM src;
    INSERT INTO src VALUES (2); -- { serverError BAD_ARGUMENTS }

    -- But its forbidden constraint must not be copied into fresh metadata.
    CREATE TABLE dst AS src; -- { serverError BAD_ARGUMENTS }
    CREATE TABLE dst CLONE AS src; -- { serverError BAD_ARGUMENTS }
    SELECT 'copy-rejected', count() FROM system.tables WHERE database = currentDatabase() AND name = 'dst';

    -- The allowed form (a direct subquery on the right-hand side of IN) is still copyable.
    CREATE TABLE okc (c0 Int, CONSTRAINT c CHECK c0 IN (SELECT c1 FROM aux)) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE okc2 AS okc;
    INSERT INTO okc2 VALUES (5);
    SELECT 'allowed-copy-ok', count() FROM okc2;
"

rm -rf "$WORK_DIR"
