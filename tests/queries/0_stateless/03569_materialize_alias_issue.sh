#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS customer_dbt_materialize"

$CLICKHOUSE_CLIENT -n --query "CREATE TABLE customer_dbt_materialize(
    key UInt64,
    value Array(Tuple(transaction_hash String, instruction_sig_hash String)) MATERIALIZED array((toString(key), toString(key)))
)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/test_replicated_merge_tree', 'customer_dbt_materialize')
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;"

$CLICKHOUSE_CLIENT --query "INSERT INTO customer_dbt_materialize SELECT number FROM numbers(1000)"

# NOTE This command looks like noop (pure metadata change which we will override with next ALTER), however it leads to important logic in the codebase:
# When we apply MODIFY COLUMN we validate that we changed something in PHYSICAL column. If we don't change anything in PHYSICAL column, we will not touch any data parts.
#
# After this MODIFY `value` column is not a physical column anymore, however it still exists in data part. So the next ALTER MODIFY COLUMN to MATERIALIZED state
# will also do nothing with data parts (because `value` is ALIAS, not PHYSICAL column).
#
# And the last MATERIALIZE COLUMN will trigger real mutation which will rewrite data part and leave incorrect checksum on disk.
$CLICKHOUSE_CLIENT --query "ALTER TABLE customer_dbt_materialize MODIFY COLUMN value Array(Tuple(transaction_hash String, instruction_sig_hash String)) ALIAS array((toString(key), toString(key))) SETTINGS mutations_sync = 2"

$CLICKHOUSE_CLIENT --query "ALTER TABLE customer_dbt_materialize MODIFY COLUMN value Array(Tuple(transaction_hash String, transaction_index_data String)) MATERIALIZED array((toString(key), toString(key)))  SETTINGS mutations_sync = 2"

$CLICKHOUSE_CLIENT --query "ALTER TABLE customer_dbt_materialize MATERIALIZE COLUMN value SETTINGS mutations_sync = 2, alter_sync = 2"

$CLICKHOUSE_CLIENT --query "SYSTEM RESTART REPLICA customer_dbt_materialize"

$CLICKHOUSE_CLIENT --query "CHECK TABLE customer_dbt_materialize"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS customer_dbt_materialize"
