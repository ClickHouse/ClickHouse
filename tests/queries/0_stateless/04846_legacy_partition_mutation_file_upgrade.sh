#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree, no-object-storage, no-shared-catalog
#
# `no-fasttest`: local-disk file surgery is not reliably available on the Fast test runner.
# `no-object-storage` / `no-shared-merge-tree` / `no-replicated-database` / `no-shared-catalog`:
# the fixture edits a real local `mutation_*.txt` file of a plain `MergeTree` table.
#
# Regression test: a legacy `mutation_*.txt` file (written before the `partition ids:` payload
# existed) is upgraded to the current format when it is loaded. Without the upgrade, every load
# resolves the `IN PARTITION` literal through the current partition key again, so a key-safe
# partition key type change (e.g. `Enum8 -> Int8`) made after the upgrade would still leave the
# table unloadable on the next restart. The legacy shape is fabricated by stripping the
# `partition ids:` line from a freshly written mutation file.

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

# Fabricate the legacy format: strip the persisted partition scope from the file.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_legacy_mutation_file"
sed -i '/^partition ids: /d' "$mutation_file"
echo "legacy 'partition ids' lines: $(grep -c '^partition ids: ' "$mutation_file")"

# Loading the legacy file resolves the scope through the (unchanged) partition key and upgrades
# the file to the current format.
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_legacy_mutation_file"
echo "upgraded 'partition ids' lines: $(grep -c '^partition ids: ' "$mutation_file")"
grep '^partition ids: ' "$mutation_file"

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
