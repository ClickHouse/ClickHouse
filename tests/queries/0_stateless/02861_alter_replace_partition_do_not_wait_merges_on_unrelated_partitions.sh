#!/usr/bin/env bash

# https://github.com/ClickHouse/ClickHouse/issues/45328
# Check that replacing one partition on a table with `ALTER TABLE REPLACE PARTITION`
# doesn't wait for merges on other partitions.

# Manually start merge (with `OPTIMIZE DEDUPLICATE`) on partition 1, 
# and at the same time, do `REPLACE PARTITION` on partition 2.

# This is a sh-test only to be able to start `OPTIMIZE DEDUPLICATE` in the background.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm <<'EOF'
-- { echo }

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1
(
    `p` UInt8,
    `i` UInt64
)
ENGINE = MergeTree
PARTITION BY p
ORDER BY tuple();

INSERT INTO t1 VALUES (1, 1), (2, 2);

SYSTEM STOP MERGES t1;

-- Creating new parts
insert into t1 (p, i) select 1, number from numbers(100);
ALTER TABLE t1 ADD COLUMN make_merge_slower UInt8 DEFAULT sleepEachRow(0.03);

CREATE TABLE t2 AS t1;
INSERT INTO t2 VALUES (2, 2000, 1);

-- expecting 2 parts for partition '1'
SELECT partition, count(partition) FROM system.parts WHERE database==currentDatabase() AND table=='t1' AND active=='1' GROUP BY partition ORDER BY partition;

SYSTEM START MERGES t1;
EOF

echo Starting merge on t1 partition '1' by \'OPTIMIZE DEDUPLICATE\'ing in a background process
# Optimize deduplicate does merge, which is supposed to take some time
# Since this is a synchronous operation, starting it in the background.
$CLICKHOUSE_CLIENT -nq "OPTIMIZE TABLE t1 PARTITION id '1' DEDUPLICATE BY p;" &
merge_pid=$!

sleep 0.1 && echo "Give the server a moment to start merge"
$CLICKHOUSE_CLIENT -nm <<'EOF'
-- { echo }
-- assume merge started
SELECT is_mutation, partition_id FROM system.merges WHERE database==currentDatabase() AND table=='t1';


-- Should complete right away since there are no merges on partition t2
ALTER TABLE t1 REPLACE PARTITION id '2' FROM t2;
SELECT * FROM t1 WHERE p=2;


-- Expecting that merge is still running
SELECT is_mutation, partition_id FROM system.merges WHERE database==currentDatabase() AND table=='t1';

-- Expecting that merge hasn't finished yet (since ALTER TABLE .. REPLACE wasn't waiting for it),
-- and there are lots of unduplicated rows
SELECT count(*) FROM t1 WHERE p=1;
EOF


# TODO: remove before merging PR
wait ${merge_pid} && echo 'Merging done'

$CLICKHOUSE_CLIENT -nm <<'EOF'
-- { echo }
-- check that merge is finished
SELECT is_mutation, partition_id FROM system.merges WHERE database==currentDatabase() AND table=='t1';

-- Expecting that merge finished
SELECT * FROM t1 ORDER BY p;
EOF
