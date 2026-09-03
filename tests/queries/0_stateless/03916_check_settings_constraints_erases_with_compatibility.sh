#!/usr/bin/env bash
# Tags: no-random-settings
# no-random-settings: because the test framework may inject the settings we are testing into the URL, which interferes with test

# checkSettingsConstraints must not erase unchanged settings, since a later `compatibility` in the same batch can change the baseline.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUERY_SKIP="SELECT value FROM system.settings WHERE name = 'use_skip_indexes_if_final'"

echo "-- compatibility alone reverts to 0"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&compatibility=24.12" -d "$QUERY_SKIP"

echo "-- explicit =1 with compatibility=24.12 (compat first in URL)"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&compatibility=24.12&use_skip_indexes_if_final=1" -d "$QUERY_SKIP"

echo "-- explicit =1 with compatibility=24.12 (setting first in URL)"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&use_skip_indexes_if_final=1&compatibility=24.12" -d "$QUERY_SKIP"

echo "-- explicit =0 with compatibility=24.12 (not affected by bug)"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&compatibility=24.12&use_skip_indexes_if_final=0" -d "$QUERY_SKIP"

echo "-- TCP: explicit =1 with compatibility=24.12"
${CLICKHOUSE_CLIENT} --compatibility=24.12 --use_skip_indexes_if_final=1 \
    -q "$QUERY_SKIP"
