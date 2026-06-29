#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for TrivialMergeSelector multi-partition indexing bug.
#
# The selector builds sorted_partition_indices (sorted by part count descending),
# but the old code read parts_ranges[partition_idx] instead of
# parts_ranges[sorted_partition_indices[partition_idx]].
#
# Scenario: partition "0" has 1 part (excluded from sorted_partition_indices
# because 1 < num_parts_to_merge=10), partition "1" has 10 parts (included at
# sorted_partition_indices[0] = 1).
#
# With the bug: parts_ranges[partition_idx=0] = partition "0" (1 part) -> too
# small -> exits loop -> no merge ever happens.
# With the fix: parts_ranges[sorted_partition_indices[0]=1] = partition "1"
# (10 parts) -> merge succeeds.

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS test_trivial_multi_partition;

CREATE TABLE test_trivial_multi_partition (x UInt64)
ENGINE = MergeTree
PARTITION BY x % 2
ORDER BY x
SETTINGS merge_selector_algorithm = 'Trivial';

-- One part in partition 0: too few to trigger merge (num_parts_to_merge = 10),
-- so partition 0 is excluded from sorted_partition_indices entirely.
INSERT INTO test_trivial_multi_partition VALUES (0);

-- Ten parts in partition 1: exactly num_parts_to_merge, so it is the only
-- entry in sorted_partition_indices (at index 0, mapping to parts_ranges[1]).
INSERT INTO test_trivial_multi_partition VALUES (1);
INSERT INTO test_trivial_multi_partition VALUES (3);
INSERT INTO test_trivial_multi_partition VALUES (5);
INSERT INTO test_trivial_multi_partition VALUES (7);
INSERT INTO test_trivial_multi_partition VALUES (9);
INSERT INTO test_trivial_multi_partition VALUES (11);
INSERT INTO test_trivial_multi_partition VALUES (13);
INSERT INTO test_trivial_multi_partition VALUES (15);
INSERT INTO test_trivial_multi_partition VALUES (17);
INSERT INTO test_trivial_multi_partition VALUES (19);

OPTIMIZE TABLE test_trivial_multi_partition;
"

# Wait until partition 1 is merged into a single active part.
# With the bug the merge never happens and this loop hangs, catching the regression.
while true
do
    result=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM system.parts
        WHERE active
          AND table = 'test_trivial_multi_partition'
          AND database = currentDatabase()
          AND partition = '1'
    ")
    [ "$result" = "1" ] && break
    sleep 0.1
done

echo "OK"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_trivial_multi_partition;"
