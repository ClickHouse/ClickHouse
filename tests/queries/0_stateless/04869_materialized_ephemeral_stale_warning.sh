#!/usr/bin/env bash
# Tags: no-shared-catalog
# no-shared-catalog: STOP MERGES will only stop them on the current replica, the second one will
# continue to merge and can materialize the mutation the on-fly case needs to stay pending

# A MATERIALIZED column reading an EPHEMERAL one cannot be recalculated outside INSERT, so a mutation
# that updates one of its other dependencies leaves it stale and says so. The warning belongs to the
# paths that write: an on-fly read builds a MutationsInterpreter per read task per part and writes
# nothing, so warning there is untrue and repeats on every read.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WARNING="depends on both EPHEMERAL"

# Prints yes/no instead of a count: the mutation may be analysed more than once per query.
warned()
{
    if grep -q "$WARNING" <<< "$1"; then echo "yes"; else echo "no"; fi
}

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_warn_alter;
CREATE TABLE t_warn_alter (id UInt64, x Int32, e Int32 EPHEMERAL 0, me Int32 MATERIALIZED x + e)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_warn_alter (id, x, e) VALUES (1, 10, 7);
"

echo -n 'alter update warns: '
warned "$($CLICKHOUSE_CLIENT --send_logs_level=warning --mutations_sync=2 \
    -q "ALTER TABLE t_warn_alter UPDATE x = 20 WHERE 1" 2>&1)"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_warn_lightweight;
CREATE TABLE t_warn_lightweight (id UInt64, x Int32, e Int32 EPHEMERAL 0, me Int32 MATERIALIZED x + e)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_warn_lightweight (id, x, e) VALUES (1, 10, 7);
"

echo -n 'lightweight update warns: '
warned "$($CLICKHOUSE_CLIENT --send_logs_level=warning --enable_lightweight_update=1 --apply_patch_parts=1 \
    -q "UPDATE t_warn_lightweight SET x = 20 WHERE 1" 2>&1)"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_warn_read;
CREATE TABLE t_warn_read (id UInt64, x Int32, e Int32 EPHEMERAL 0, me Int32 MATERIALIZED x + e)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_warn_read (id, x, e) VALUES (1, 10, 7);
SYSTEM STOP MERGES t_warn_read;
ALTER TABLE t_warn_read UPDATE x = 20 WHERE 1 SETTINGS alter_sync = 0, mutations_sync = 0, send_logs_level = 'error';
"

# The mutation stays pending, so this read applies it on the fly. `me` keeps its stored value, which is
# what the warning above is about, and the read must not repeat it.
echo -n 'on-fly read warns: '
warned "$($CLICKHOUSE_CLIENT --send_logs_level=warning --apply_mutations_on_fly=1 \
    -q "SELECT x, me FROM t_warn_read" 2>&1)"

echo -n 'on-fly read value: '
$CLICKHOUSE_CLIENT --apply_mutations_on_fly=1 -q "SELECT x, me FROM t_warn_read"

$CLICKHOUSE_CLIENT -q "
SYSTEM START MERGES t_warn_read;
DROP TABLE t_warn_alter;
DROP TABLE t_warn_lightweight;
DROP TABLE t_warn_read;
"
