#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree, no-object-storage, no-shared-catalog
#
# `no-fasttest`: local-disk file surgery is not reliably available on the Fast test runner.
# `no-object-storage` / `no-shared-merge-tree` / `no-replicated-database` / `no-shared-catalog`:
# the fixture edits a real local `mutation_*.txt` file of a plain `MergeTree` table.
#
# Regression test: a legacy `mutation_*.txt` file (written before the partition scope of the
# commands was pinned to `IN PARTITION ID`) is rewritten when it is loaded. Without that
# rewrite, every load resolves the `IN PARTITION` literal through the current partition key
# again, so a key-safe partition key type change (e.g. `Enum8 -> Int8`) made after the load
# would still leave the table unloadable on the next restart. The legacy shape is fabricated by
# turning the pinned `IN PARTITION ID '1'` of a freshly written mutation file back into the
# original `IN PARTITION 'a'` literal. Note that the file keeps the shape it always had, so a
# rewritten file is still readable by a binary without this feature.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_legacy_mutation_file"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_legacy_mutation_file (p Enum8('a' = 1, 'b' = 2), n Int64)
    ENGINE = MergeTree PARTITION BY p ORDER BY tuple()"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_legacy_mutation_file VALUES ('a', 1), ('b', 2)"

# The mutation file of a finished mutation is kept on disk and is read back by `loadMutations`
# on every ATTACH, exactly like the file of a pending one, so the mutation does not have to be
# kept pending for the loading path under test to be exercised.
${CLICKHOUSE_CLIENT} --query "
    ALTER TABLE t_legacy_mutation_file UPDATE n = n + 100 IN PARTITION 'a' WHERE 1
    SETTINGS mutations_sync = 2"

data_dir=$(${CLICKHOUSE_CLIENT} --query "
    SELECT data_paths[1] FROM system.tables
    WHERE database = currentDatabase() AND name = 't_legacy_mutation_file'")
mutation_file=$(ls "$data_dir"mutation_*.txt)

# Fabricate the legacy format: turn the pinned partition id back into the original literal.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_legacy_mutation_file"
# The command text of the file quotes the literals, so the partition id appears as `ID \'1\'`.
sed -i "s|IN PARTITION ID \\\\'1\\\\'|IN PARTITION \\\\'a\\\\'|" "$mutation_file"
echo "legacy 'IN PARTITION ID' occurrences: $(grep -c -F 'IN PARTITION ID ' "$mutation_file")"

# Loading the legacy file resolves the scope through the (unchanged) partition key and rewrites
# the file with the scope pinned.
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_legacy_mutation_file"
echo "upgraded 'IN PARTITION ID' occurrences: $(grep -c -F 'IN PARTITION ID ' "$mutation_file")"

# A key-safe metadata change of the partition key column: `Enum8 -> Int8` keeps the numeric
# on-disk partition id, but re-parsing the literal 'a' as `Int8` would throw.
${CLICKHOUSE_CLIENT} --query "
    ALTER TABLE t_legacy_mutation_file MODIFY COLUMN p Int8 SETTINGS alter_sync = 2"

# Without the upgrade this reattach would fail to load the table: the legacy file would come
# through the fallback again and re-parse the stale literal against the new key type.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_legacy_mutation_file"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_legacy_mutation_file"

${CLICKHOUSE_CLIENT} --query "SELECT p, n FROM t_legacy_mutation_file ORDER BY p, n"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_legacy_mutation_file' AND NOT is_done"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_legacy_mutation_file"
