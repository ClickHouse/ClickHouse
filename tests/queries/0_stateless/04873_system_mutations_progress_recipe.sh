#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tests the `mutation_progress` view documented in `docs/reference/system-tables/mutations.mdx`.
# The view is extracted from the documentation instead of being copied here, so that the two cannot
# drift: a copy that fell behind would keep asserting the old behaviour and hide the regression.
# `system.*` is redirected to mock tables, because the states worth covering (an unfinished mutation
# with nothing to do, a part queued on a lagging replica, a part rewritten for an earlier mutation)
# cannot be staged deterministically against a live server.

DOCS="$CUR_DIR/../../../docs/reference/system-tables/mutations.mdx"

VIEW_SQL=$(awk '/^CREATE VIEW mutation_progress AS$/ { found = 1 }
                found { if ($0 == "```") exit; print }' "$DOCS")

if [ -z "$VIEW_SQL" ]; then
    echo "Could not extract 'CREATE VIEW mutation_progress' from $DOCS"
    exit 1
fi

# Each scenario uses its own value of the `table` column. The view filters every one of its
# subqueries by database and table, so this isolates them; sharing one table would let the
# single-block-number fallback of a non-replicated mutation pull in another scenario's parts.
VIEW_SQL=${VIEW_SQL//system.mutations/mock_mutations}
VIEW_SQL=${VIEW_SQL//system.parts/mock_parts}
VIEW_SQL=${VIEW_SQL//system.merges/mock_merges}

$CLICKHOUSE_CLIENT -q "
    DROP VIEW IF EXISTS mutation_progress;
    DROP TABLE IF EXISTS mock_mutations;
    DROP TABLE IF EXISTS mock_parts;
    DROP TABLE IF EXISTS mock_merges;

    CREATE TABLE mock_mutations
    (
        database String, table String, mutation_id String, parts_to_do Int64,
        parts_to_do_names Array(String), parts_in_progress_names Array(String),
        \`block_numbers.partition_id\` Array(String), \`block_numbers.number\` Array(Int64),
        is_done UInt8
    ) ENGINE = Memory;

    CREATE TABLE mock_parts
    (
        database String, table String, name String, partition_id String,
        min_block_number Int64, bytes_on_disk UInt64, active UInt8
    ) ENGINE = Memory;

    CREATE TABLE mock_merges
    (
        database String, table String, source_part_names Array(String),
        progress Float64, is_mutation UInt8
    ) ENGINE = Memory;

    -- An unfinished mutation with nothing to do is waiting for an in-flight INSERT.
    INSERT INTO mock_mutations VALUES ('db', 'no_parts_to_do', 'm', 0, [], [], ['p'], [9], 0);

    -- A part queued on a lagging replica has no \`system.parts\` row, so its size is unknown.
    INSERT INTO mock_parts VALUES ('db', 'part_not_local', 'known', 'all', 1, 100, 1);
    INSERT INTO mock_mutations VALUES ('db', 'part_not_local', 'm', 2, ['known', 'queued'], [], [''], [9], 0);

    -- Control for the case above: same shape, but every remaining part is on disk.
    INSERT INTO mock_parts VALUES ('db', 'all_local', 'known', 'all', 1, 100, 1), ('db', 'all_local', 'other', 'all', 2, 100, 1);
    INSERT INTO mock_mutations VALUES ('db', 'all_local', 'm', 2, ['known', 'other'], [], [''], [9], 0);

    -- A multi-command mutation repeats its rows and must still be counted once.
    INSERT INTO mock_parts VALUES ('db', 'multi_command', 'a', 'all', 8, 400, 1);
    INSERT INTO mock_mutations VALUES ('db', 'multi_command', 'm', 1, ['a'], [], [''], [9], 0),
                                     ('db', 'multi_command', 'm', 1, ['a'], [], [''], [9], 0);

    -- A part inserted after the mutation was submitted has a higher block number and is out of scope,
    -- so it must not make a mutation that has rewritten nothing look almost finished.
    INSERT INTO mock_parts VALUES ('db', 'later_insert', 'a', 'all', 1, 100, 1), ('db', 'later_insert', 'b', 'all', 3, 900, 1);
    INSERT INTO mock_mutations VALUES ('db', 'later_insert', 'm', 1, ['a'], [], [''], [2], 0);

    -- A replicated mutation carries one block number per partition: p0 is cut at 5 and p1 at 9,
    -- so the scope is 50 + 70 and the 600 and 800 byte parts are excluded.
    INSERT INTO mock_parts VALUES ('db', 'per_partition', 'c', 'p0', 2, 50, 1), ('db', 'per_partition', 'd', 'p1', 4, 70, 1),
                                  ('db', 'per_partition', 'e', 'p0', 6, 600, 1), ('db', 'per_partition', 'f', 'p1', 30, 800, 1);
    INSERT INTO mock_mutations VALUES ('db', 'per_partition', 'm', 1, ['c'], [], ['p0', 'p1'], [5, 9], 0);

    -- Part x is half rewritten, but that rewrite only advances \`credited\`. \`not_credited\` also lists
    -- x as remaining, and must not be given the same live fraction.
    INSERT INTO mock_parts VALUES ('db', 'in_progress_gate', 'x', 'p', 1, 1000, 1);
    INSERT INTO mock_merges VALUES ('db', 'in_progress_gate', ['x'], 0.5, 1);
    INSERT INTO mock_mutations VALUES ('db', 'in_progress_gate', 'credited', 1, ['x'], ['x'], ['p'], [2], 0),
                                      ('db', 'in_progress_gate', 'not_credited', 1, ['x'], [], ['p'], [3], 0);

    $VIEW_SQL
"

for scenario in no_parts_to_do part_not_local all_local multi_command later_insert per_partition in_progress_gate; do
    $CLICKHOUSE_CLIENT -q "
        SELECT '$scenario', mutation_id, parts_to_do, bytes_left, progress
        FROM mutation_progress(database = 'db', table = '$scenario')"
done

$CLICKHOUSE_CLIENT -q "
    DROP VIEW mutation_progress;
    DROP TABLE mock_mutations;
    DROP TABLE mock_parts;
    DROP TABLE mock_merges;"
