#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./02500_remove_redundant_distinct.sh
ENABLE_ANALYZER=1 . "$CURDIR"/02500_remove_redundant_distinct.sh
