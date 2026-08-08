#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `CHECK` constraint containing a forbidden subquery is rejected on `CREATE TABLE`,
# but metadata created before that validation existed must keep loading, and a backup
# of such a table must stay restorable: `RESTORE` deliberately skips the fresh-definition
# validation (like the other `is_restore_from_backup` relaxations in
# `InterpreterCreateQuery`), because rejecting the definition would make existing backups
# unrestorable. The restored table behaves exactly like the grandfathered original: it
# loads and is readable, the first `INSERT` fails when the constraint is compiled, and
# `ALTER TABLE ... DROP CONSTRAINT` recovers it.
#
# To obtain a grandfathered table, create it with a valid constraint in a
# `clickhouse local` session with a persistent path, rewrite the constraint in the
# stored metadata file to a forbidden one, and start a second session (the same
# staging as in `04759_create_as_grandfathered_check_constraint_subquery.sh`).

WORK_DIR=$CLICKHOUSE_TMP/04816_restore_grandfathered
rm -rf "$WORK_DIR"

$CLICKHOUSE_LOCAL --path "$WORK_DIR" < /dev/null --query "
    CREATE TABLE aux (c1 Int) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO aux VALUES (5);
    CREATE TABLE src (c0 Int, CONSTRAINT c CHECK c0 > 0) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO src VALUES (1);
"

sed -i "s/CHECK c0 > 0/CHECK c0 + (SELECT max(c1) FROM default.aux) > 0/" "$WORK_DIR"/store/*/*/src.sql

$CLICKHOUSE_LOCAL --path "$WORK_DIR" < /dev/null --query "
    SELECT 'load-ok', count() FROM src;

    -- A backup of the grandfathered table must stay restorable.
    BACKUP TABLE src TO Memory('backup_04816') FORMAT Null;
    RESTORE TABLE src AS dst FROM Memory('backup_04816') FORMAT Null;
    SELECT 'restore-ok', count() FROM dst;

    -- The restored table behaves like the grandfathered original: the constraint
    -- fails when it is compiled on the first INSERT.
    INSERT INTO dst VALUES (2); -- { serverError BAD_ARGUMENTS }

    -- Dropping the constraint recovers the table.
    ALTER TABLE dst DROP CONSTRAINT c;
    INSERT INTO dst VALUES (2);
    SELECT 'after-drop-ok', count() FROM dst;
"

rm -rf "$WORK_DIR"
