#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
#
# A non-transactional `DETACH` copies the parts to `detached/` only after the removal has gone
# through, so a removal that is refused cannot leave an orphan copy behind. This checks the other
# half of that contract: the copies are still made, and made for every part that the removal
# actually took, for both `DETACH PARTITION` and `DETACH PART`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_detach_clone"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_detach_clone (p UInt64, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_detach_clone SETTINGS async_insert = 0 VALUES (1, 1)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_detach_clone SETTINGS async_insert = 0 VALUES (1, 2)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_detach_clone SETTINGS async_insert = 0 VALUES (2, 3)"

# One part of partition 2 is detached by name, both parts of partition 1 by partition.
part_of_2=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_detach_clone' AND partition = '2' AND active")
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_detach_clone DETACH PART '$part_of_2'"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_detach_clone DETACH PARTITION 1"

echo "rows left: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM t_detach_clone")"
echo "detached: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_detach_clone'")"

# The copies are complete: re-attaching them brings every row back.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_detach_clone ATTACH PARTITION 1"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_detach_clone ATTACH PARTITION 2"
echo "rows reattached: $($CLICKHOUSE_CLIENT -q "SELECT sum(x) FROM t_detach_clone")"

# A `DROP` on the same path must not copy anything.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_detach_clone DROP PARTITION 1"
echo "detached after drop: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_detach_clone'")"
echo "rows after drop: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM t_detach_clone")"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_detach_clone"
