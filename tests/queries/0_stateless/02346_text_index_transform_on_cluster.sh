#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# no-replicated-database: ON CLUSTER is not allowed for a Replicated database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The hosts of an `ON CLUSTER` query resolve a text index transform against their own schema and
# without the submitting user, so it is refused unless they run as that user.

USER="user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -n -q "
    DROP USER IF EXISTS $USER;
    CREATE TABLE tab (id UInt64, val String, lowered String ALIAS lower(val)) ENGINE = MergeTree ORDER BY id;
"

echo '1. ALTER ADD INDEX with a transform is refused.'
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode none -q "
    ALTER TABLE tab ON CLUSTER test_shard_localhost
        ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lowered)
" 2>&1 | grep -c -m1 "distributed_ddl_use_initial_user_and_roles"

echo '2. CREATE with a transform is refused.'
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode none -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.dst_transform ON CLUSTER test_shard_localhost
    (id UInt64, val String, INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val)))
    ENGINE = MergeTree ORDER BY id
" 2>&1 | grep -c -m1 "distributed_ddl_use_initial_user_and_roles"

echo '3. An index without a transform is unaffected.'
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode none -q "
    ALTER TABLE tab ON CLUSTER test_shard_localhost
        ADD INDEX idx_plain(val) TYPE text(tokenizer = 'splitByNonAlpha')
"
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab' ORDER BY name"

# With an explicit engine `setEngine` does not look at `AS src`, and the inherited indices are inspected
# on the initiator, so describing the source has to be authorized first.
$CLICKHOUSE_CLIENT -n -q "
    CREATE USER $USER;
    GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.dst TO $USER;
    GRANT TABLE ENGINE ON MergeTree TO $USER;
"

echo '4. CREATE AS with an explicit engine and no privilege on the source.'
$CLICKHOUSE_CLIENT --user "$USER" --distributed_ddl_output_mode none -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.dst ON CLUSTER test_shard_localhost AS ${CLICKHOUSE_DATABASE}.tab ENGINE = MergeTree ORDER BY tuple()
" 2>&1 | grep -o -m1 "SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.tab"

echo '5. The same for a source that does not exist.'
$CLICKHOUSE_CLIENT --user "$USER" --distributed_ddl_output_mode none -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.dst ON CLUSTER test_shard_localhost AS ${CLICKHOUSE_DATABASE}.missing ENGINE = MergeTree ORDER BY tuple()
" 2>&1 | grep -o -m1 "SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.missing"

$CLICKHOUSE_CLIENT -n -q "
    DROP USER $USER;
    DROP TABLE tab;
"
