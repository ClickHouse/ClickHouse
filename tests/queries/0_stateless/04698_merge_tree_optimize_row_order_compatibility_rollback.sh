#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Guards the `compatibility` rollback path of `optimize_row_order_if_no_order_by`: below the
# version that flipped the default, a table that omits the setting must behave as if it were 0,
# i.e. preserve the insertion order. `03999_stateless_settings_history.sh` only checks the current
# default against the latest `new_value` of the `SettingsChangesHistory` entry, so it would not
# catch a wrong `previous_value` there.
#
# `compatibility` is applied to `MergeTree` settings once, when the server's global
# `MergeTreeSettings` are materialized (`Context::getMergeTreeSettings`), so it has to come from
# the default profile rather than from a `SET` in an already-running session. `clickhouse local`
# with `--compatibility` gives exactly that: a fresh global context with the value in place.

ROWS="('Bob', 4, 100, '1'), ('Nikita', 2, 54, '1'), ('Nikita', 1, 228, '1'), ('Alex', 4, 83, '1'), ('Alex', 4, 134, '1'), ('Alex', 1, 65, '0'), ('Alex', 4, 134, '1'), ('Bob', 2, 53, '0'), ('Alex', 4, 83, '0'), ('Alex', 1, 63, '1'), ('Bob', 2, 53, '1'), ('Alex', 4, 192, '1'), ('Alex', 2, 128, '1'), ('Nikita', 2, 148, '0'), ('Bob', 4, 177, '0'), ('Nikita', 1, 173, '0'), ('Alex', 1, 239, '0'), ('Alex', 1, 63, '0'), ('Alex', 2, 224, '1'), ('Bob', 4, 177, '0')"

run_case()
{
    # $1 - extra command line arguments for `clickhouse local`
    # shellcheck disable=SC2086
    ${CLICKHOUSE_LOCAL} $1 --multiquery --query "
        SET max_insert_threads = 1;
        -- The oracle below compares against the on-disk row order, so the read must be
        -- single-streamed: with more than one stream, groupArray would reflect stream
        -- interleaving rather than the physical order.
        SET max_threads = 1;

        SELECT value FROM system.merge_tree_settings WHERE name = 'optimize_row_order_if_no_order_by';

        CREATE TABLE tab (
            name String,
            timestamp Int64,
            money UInt8,
            flag String
        ) ENGINE = MergeTree
        ORDER BY ()
            -- Disable add_minmax_index_for_numeric_columns since it affects the order
        SETTINGS add_minmax_index_for_numeric_columns = 0;

        INSERT INTO tab VALUES ${ROWS};

        SELECT
            (SELECT groupArray((name, timestamp, money, flag)) FROM (SELECT * FROM tab))
            =
            [${ROWS}]::Array(Tuple(String, Int64, UInt8, String));
    "
}

echo 'compatibility 26.7: setting value, insertion order preserved'
run_case "--compatibility 26.7"

echo 'current default: setting value, insertion order preserved'
run_case ""
