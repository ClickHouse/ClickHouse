#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

$CLICKHOUSE_CLIENT --multiquery --query "
    DROP TABLE IF EXISTS alter_broken_indices;
    CREATE TABLE alter_broken_indices
    (
        x UInt64,
        y UInt64,
        INDEX idx_first x TYPE minmax,
        INDEX idx_unrelated y TYPE minmax,
        INDEX idx_second (x + 1) TYPE minmax,
        INDEX idx_third (x * 2) TYPE minmax
    ) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS add_minmax_index_for_numeric_columns = 0;
    INSERT INTO alter_broken_indices VALUES (3, 7);
"

check_drop_error()
{
    local query="$1"
    local affected_indices="$2"
    local error
    local index
    if error=$($CLICKHOUSE_CLIENT --query "$query" 2>&1); then
        echo 'Expected the column deletion to fail'
        exit 1
    fi

    # The original error code of the first broken index is preserved (here `UNKNOWN_IDENTIFIER`,
    # because dropping the column leaves the index expression referencing a missing column).
    [[ "$error" == *'Code: 47.'* ]]
    [[ "$error" == *'UNKNOWN_IDENTIFIER'* ]]
    [[ "$error" == *'Cannot apply ALTER because it breaks skip indices:'* ]]
    for index in idx_first idx_second idx_third idx_unrelated; do
        if [[ " $affected_indices " == *" $index "* ]]; then
            [[ "$error" == *"${index}: "* ]]
        else
            [[ "$error" != *"${index}: "* ]]
        fi
    done
}

check_drop_error 'ALTER TABLE alter_broken_indices DROP COLUMN x' \
    'idx_first idx_second idx_third'
echo 'All three dependent indices reported'

# An index removed in the same `ALTER` must not appear among the failures.
check_drop_error 'ALTER TABLE alter_broken_indices DROP INDEX idx_first, DROP COLUMN x' \
    'idx_second idx_third'
echo 'Only retained dependent indices reported'

# A single failure uses the same index-list format.
check_drop_error 'ALTER TABLE alter_broken_indices DROP INDEX idx_first, DROP INDEX idx_second, DROP COLUMN x' \
    'idx_third'
echo 'Single-index error includes index list'

check_policy_error()
{
    local error
    if error=$($CLICKHOUSE_CLIENT --query "ALTER TABLE alter_broken_indices $1" 2>&1); then
        echo 'Expected the column alteration to fail'
        exit 1
    fi

    [[ "$error" == *'Code: 524.'* ]]
    [[ "$error" == *'ALTER_OF_COLUMN_IS_FORBIDDEN'* ]]
    [[ "$error" == *'used by the indices idx_first, idx_second, idx_third. Check'* ]]
    [[ "$error" != *'idx_unrelated'* ]]
}

$CLICKHOUSE_CLIENT --query "ALTER TABLE alter_broken_indices MODIFY SETTING alter_column_secondary_index_mode = 'throw'"
check_policy_error 'RENAME COLUMN x TO renamed_x'
echo 'Rename reports all dependent indices'
check_policy_error 'CLEAR COLUMN x'
echo 'Clear reports all dependent indices'
check_policy_error 'MODIFY COLUMN x UInt32'
echo 'Modify reports all dependent indices'

# Rejected changes leave both the data and all index definitions intact.
$CLICKHOUSE_CLIENT --multiquery --query "
    SELECT x, y FROM alter_broken_indices;
    SELECT name FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 'alter_broken_indices' ORDER BY name;

    ALTER TABLE alter_broken_indices
        DROP COLUMN x, DROP INDEX idx_first, DROP INDEX idx_second, DROP INDEX idx_third;
    SELECT y FROM alter_broken_indices;
    SELECT name FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 'alter_broken_indices' ORDER BY name;
    DROP TABLE alter_broken_indices;
"
