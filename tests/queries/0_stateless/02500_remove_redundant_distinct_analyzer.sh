#!/usr/bin/env bash
# Tags: long
# Tag long: distributed remote() EXPLAIN cases push flaky-check repeated runs past 180s

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./02500_remove_redundant_distinct.sh
ENABLE_ANALYZER=1 . "$CURDIR"/02500_remove_redundant_distinct.sh
