#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-ordinary-database, no-shared-merge-tree
# Looks like server does not listen https port in fasttest
# FIXME Replicated database executes ALTERs in separate context, so transaction info is lost

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib
set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS table_with_rolled_back_gap SYNC;"

# max_bytes_to_merge_at_max_space_in_pool = 0 disables background merges while leaving explicit
# OPTIMIZE working. A rolled back part is removable as soon as it appears, independently of
# old_parts_lifetime, so the fixture only survives while the parts cleanup does not run:
# remove_rolled_back_parts_immediately turns that off, and the interval settings keep the
# cleanup task away from the fixture even if it does run.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE table_with_rolled_back_gap (n UInt64) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS old_parts_lifetime = 10000, max_bytes_to_merge_at_max_space_in_pool = 0,
             remove_rolled_back_parts_immediately = 0,
             merge_tree_clear_old_parts_interval_seconds = 100000,
             cleanup_delay_period = 100000, max_cleanup_delay_period = 100000;"

$CLICKHOUSE_CLIENT --query "INSERT INTO table_with_rolled_back_gap VALUES (1);"
$CLICKHOUSE_CLIENT --query "INSERT INTO table_with_rolled_back_gap VALUES (2);"

# Merges the two parts inside a transaction and rolls it back, which leaves all_1_2_1 Outdated
# with creation_csn = Tx::RolledBackCSN. Two more parts are added afterwards, so the merge below
# spans a wider range than the rolled back part and gets a different name.
tx 1 "begin transaction"
tx 1 "optimize table table_with_rolled_back_gap partition tuple() final settings optimize_throw_if_noop = 1"
tx 1 "rollback"

$CLICKHOUSE_CLIENT --query "INSERT INTO table_with_rolled_back_gap VALUES (3);"
$CLICKHOUSE_CLIENT --query "INSERT INTO table_with_rolled_back_gap VALUES (4);"

# 18446744073709551615 is Tx::RolledBackCSN. Asserts the shape the merge below has to accept:
# a single outdated part whose level exceeds every active part's, left behind by a rolled back
# transaction. Without that part the merge is unconditionally allowed and proves nothing.
$CLICKHOUSE_CLIENT --query "
    SELECT 'gap part is a rolled back merge',
           countIf(NOT active) == 1,
           maxIf(level, NOT active) > maxIf(level, active),
           minIf(creation_csn, NOT active) == 18446744073709551615
    FROM system.parts
    WHERE table = 'table_with_rolled_back_gap' AND database = currentDatabase();"

tx 2 "begin transaction"
tx 2 "optimize table table_with_rolled_back_gap partition tuple() final settings optimize_throw_if_noop = 1"
tx 2 "commit"

$CLICKHOUSE_CLIENT --query "SELECT 'active parts after the merge';"
$CLICKHOUSE_CLIENT --query "
    SELECT name, level FROM system.parts
    WHERE table = 'table_with_rolled_back_gap' AND database = currentDatabase() AND active
    ORDER BY min_block_number, max_block_number, level, name;"

# The merged part intersects the rolled back one, so this is what proves allowing the merge is
# safe: loading resolves such a pair in favour of the committed part instead of reporting it.
$CLICKHOUSE_CLIENT --query "DETACH TABLE table_with_rolled_back_gap;"
$CLICKHOUSE_CLIENT --query "ATTACH TABLE table_with_rolled_back_gap;"
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT LOADING PARTS table_with_rolled_back_gap;"

$CLICKHOUSE_CLIENT --query "
    SELECT 'rows after detach/attach', count(), sum(n) FROM table_with_rolled_back_gap;"

$CLICKHOUSE_CLIENT --query "DROP TABLE table_with_rolled_back_gap SYNC;"
