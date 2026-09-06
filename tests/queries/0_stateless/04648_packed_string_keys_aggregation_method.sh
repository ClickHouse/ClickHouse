#!/usr/bin/env bash
# Tags: no-random-settings
# `no-random-settings`: the last case asserts what `compatibility` picks for
# `enable_packed_string_keys_in_aggregation`, and an explicitly randomized value overrides
# `compatibility` by design, so the legacy method is never selected.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Check that `enable_packed_string_keys_in_aggregation` switches the aggregation method
# for a single non-nullable String key between `key_packed_string` and the legacy `key_string`.

query="SELECT count() FROM (SELECT toString(number % 4) AS s FROM numbers(65536)) GROUP BY s FORMAT Null"

function aggregation_method()
{
    $CLICKHOUSE_CLIENT --send_logs_level=trace "$1" -q "$query" 2>&1 \
        | grep -oE 'Aggregation method: [a-z_0-9]+' | sort -u
}

aggregation_method "--enable_packed_string_keys_in_aggregation=1"
aggregation_method "--enable_packed_string_keys_in_aggregation=0"
# `compatibility` with pre-26.8 versions must select the legacy method.
aggregation_method "--compatibility=25.6"
