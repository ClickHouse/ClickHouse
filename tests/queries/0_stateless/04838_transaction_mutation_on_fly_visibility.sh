#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# Tag rationale: uses explicit transactions, which require an Atomic,
# non-replicated database.

# A transactional mutation is applied only to the data parts visible to its
# transaction's snapshot: `selectPartsToMutate` skips every other part as
# `VERSION_NOT_VISIBLE`, so such a part is never mutated on disk. On-fly
# application (`apply_mutations_on_fly`) used to decide purely by comparing the
# mutation version to the part's data version, so it applied a committed
# transactional mutation to a part with a smaller data version that was
# committed by another transaction *after* the mutation's snapshot - returning
# data the mutation never touched, and data that a plain `SELECT` does not show.
#
# Here transaction 1 creates a part while transaction 2 starts and commits a
# mutation with a larger version; transaction 1 commits only afterwards, so its
# part is invisible to the mutation and must keep its original value both with
# and without `apply_mutations_on_fly`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_on_fly_visibility;
    CREATE TABLE t_on_fly_visibility (k UInt64, v UInt64)
        ENGINE = MergeTree ORDER BY k
        SETTINGS finished_mutations_to_keep = 100, old_parts_lifetime = 3600;
"

# A committed part, visible to the mutation below: it gets mutated on disk.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_on_fly_visibility VALUES (1, 1)"

# Transaction 1 creates a part with a data version smaller than the mutation
# version allocated below, but does not commit it yet.
tx 1 "BEGIN TRANSACTION"
tx 1 "INSERT INTO t_on_fly_visibility SETTINGS async_insert = 0 VALUES (2, 2)"

# Transaction 2 mutates. Its snapshot does not contain transaction 1's part, so
# the mutation will never touch that part. A transactional mutation is always
# synchronous, so the `ALTER` returns only after the mutation is applied to
# every part it can see.
tx 2 "BEGIN TRANSACTION"
tx 2 "ALTER TABLE t_on_fly_visibility UPDATE v = 1000 WHERE 1"
tx 2 "COMMIT"

# Keep the two parts apart: once transaction 1 commits, a merge could produce a
# single part whose data version is already above the mutation version, which
# would hide the behaviour this test is about. Merges have to stay enabled until
# here, because the mutation above is executed by the same background pool.
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_on_fly_visibility"

# Transaction 1 commits only now, after transaction 2's snapshot.
tx 1 "COMMIT"

# The mutation entry has to be still around, otherwise the assertions below are
# vacuous: an entry that is gone from `current_mutations_by_version` is not part
# of the on-fly mutations snapshot at all.
echo "lingering mutation entries: $($CLICKHOUSE_CLIENT -q "
    SELECT count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_on_fly_visibility'")"

echo "plain"
$CLICKHOUSE_CLIENT -q "SELECT k, v FROM t_on_fly_visibility ORDER BY k SETTINGS apply_mutations_on_fly = 0"
echo "on the fly"
$CLICKHOUSE_CLIENT -q "SELECT k, v FROM t_on_fly_visibility ORDER BY k SETTINGS apply_mutations_on_fly = 1"

$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_on_fly_visibility"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_on_fly_visibility"
