#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: fails due to additional shard.

# A heavy mutation rewrites regular parts only, so a pending one owes nothing to a patch part and must
# not stop it from being removed. The patch partition id carries a structure hash and can only be read
# at runtime, which is why this is not a .sql test. See #120902.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_dpum_patch_part SYNC" ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_dpum_patch_part SYNC"

# Both block-number settings are pinned because the test runner randomizes them, and a patch part
# cannot exist without them.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_dpum_patch_part (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1"

$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_dpum_patch_part"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_dpum_patch_part VALUES (1, 10)"

# alter_update_mode is pinned on both ALTERs because the stress runner randomizes it as a client
# option: the first one has to leave a patch part, the second a heavy mutation that stays pending.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_dpum_patch_part UPDATE v = v + 100 WHERE 1
    SETTINGS enable_lightweight_update = 1, alter_update_mode = 'lightweight_force'"

patch_partition_id=$($CLICKHOUSE_CLIENT -q "SELECT any(partition_id) FROM system.parts
    WHERE database = currentDatabase() AND table = 't_dpum_patch_part' AND active AND startsWith(partition_id, 'patch-')")

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_dpum_patch_part UPDATE v = v + 1000 WHERE 1
    SETTINGS mutations_sync = 0, alter_update_mode = 'heavy'"

# The armed state is asserted first, so neither refusal check below can pass vacuously against a table
# that grew no patch part or against a mutation that already finished.
echo "patch parts $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_dpum_patch_part' AND active AND startsWith(partition_id, 'patch-')")"
echo "mutation armed $($CLICKHOUSE_CLIENT -q "SELECT is_done, parts_to_do FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_dpum_patch_part' AND NOT is_done")"

# The claim: the pending mutation does not reach the patch partition, so this must be allowed.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_dpum_patch_part DETACH PARTITION ID '$patch_partition_id'" \
    > "$CLICKHOUSE_TMP"/05231_detach_patch.txt 2>&1
if grep -q 'Code:' "$CLICKHOUSE_TMP"/05231_detach_patch.txt; then echo "detach_patch_refused 1"; else echo "detach_patch_refused 0"; fi

# The base partition is still refused in the same state, so the check above cannot pass with the guard
# removed altogether.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_dpum_patch_part DETACH PARTITION ID 'all'" \
    > "$CLICKHOUSE_TMP"/05231_detach_base.txt 2>&1
if grep -q 'has not been applied to part' "$CLICKHOUSE_TMP"/05231_detach_base.txt; then echo "detach_base_refused_by_guard 1"; else echo "detach_base_refused_by_guard 0"; fi

# A detached patch partition is replaced by an empty patch part over the same block range, so what the
# read returns is the oracle for the detach having taken effect, not the number of patch parts.
echo "detached parts $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.detached_parts
    WHERE database = currentDatabase() AND table = 't_dpum_patch_part'")"
echo "sum after patch detach $($CLICKHOUSE_CLIENT -q "SELECT sum(v) FROM t_dpum_patch_part
    SETTINGS apply_mutations_on_fly = 0")"
echo "base parts $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_dpum_patch_part' AND active AND partition_id = 'all'")"
echo "mutation still pending $($CLICKHOUSE_CLIENT -q "SELECT countIf(NOT is_done) FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_dpum_patch_part'")"
