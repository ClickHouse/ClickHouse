#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree
# no-replicated-database: the test relies on the block numbers and the mutation version of a single, freshly created table.
# no-shared-merge-tree: the test is about the covering part that `ReplicatedMergeTree` writes on `REPLACE PARTITION`.

# The `ReplicatedMergeTree` counterpart of `05229_replace_partition_covers_outdated_parts`.
# `REPLACE PARTITION` writes an empty part that covers the replaced ones, so that a restart before they
# are unlinked does not resurrect them. Parts that were outdated earlier can still be on disk as well,
# and the covering part has to contain those too - otherwise the part loader finds a pair of parts that
# neither contain one another nor are disjoint, and the table fails to attach with
# "Part ... intersects previous part ...".
#
# Here the leftover is a mutated part `1_1_1_0_2` removed by `DROP PART`, while the mutation is stuck
# on the only remaining active part `1_0_0_0`. The covering part must take the mutation version of the
# leftover: `1_0_3_1` does not contain `1_1_1_0_2`, `1_0_3_1_2` does.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_replace_cover_rep SYNC;
    DROP TABLE IF EXISTS t_replace_cover_rep_src;

    CREATE TABLE t_replace_cover_rep (p UInt64, k UInt64, v UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_replace_cover_rep', 'r1')
    PARTITION BY p ORDER BY k SETTINGS old_parts_lifetime = 100000;

    CREATE TABLE t_replace_cover_rep_src (p UInt64, k UInt64, v UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;

    -- Keep the outdated parts on disk until the simulated restart.
    SYSTEM STOP CLEANUP t_replace_cover_rep;

    -- A retry after an injected Keeper fault allocates a new block number and would shift the part names.
    INSERT INTO t_replace_cover_rep SETTINGS async_insert = 0, insert_keeper_fault_injection_probability = 0 VALUES (1, 1, 0);
    INSERT INTO t_replace_cover_rep SETTINGS async_insert = 0, insert_keeper_fault_injection_probability = 0 VALUES (1, 2, 0);

    -- The mutation succeeds on \`1_1_1_0\` and keeps failing on \`1_0_0_0\`.
    ALTER TABLE t_replace_cover_rep UPDATE v = v + throwIf(k = 1) WHERE 1 SETTINGS mutations_sync = 0;
"

for _ in {1..600}
do
    [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_replace_cover_rep' AND name = '1_1_1_0_2' AND active") == 1 ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "
    -- A part in the middle of the partition is dropped without a covering part, so \`1_1_1_0_2\` stays on disk
    -- as an outdated part that nothing active covers.
    ALTER TABLE t_replace_cover_rep DROP PART '1_1_1_0_2';

    INSERT INTO t_replace_cover_rep_src VALUES (1, 3, 0);

    ALTER TABLE t_replace_cover_rep REPLACE PARTITION 1 FROM t_replace_cover_rep_src;

    SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_replace_cover_rep' AND name = '1_1_1_0_2';

    -- DETACH + ATTACH rebuilds the set of parts from disk, just like a server restart.
    DETACH TABLE t_replace_cover_rep;
    ATTACH TABLE t_replace_cover_rep;

    SELECT * FROM t_replace_cover_rep ORDER BY k;

    DROP TABLE t_replace_cover_rep SYNC;
    DROP TABLE t_replace_cover_rep_src;
"
