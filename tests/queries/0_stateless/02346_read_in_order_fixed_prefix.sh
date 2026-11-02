#!/usr/bin/env bash
# Tags: no-fasttest, long
# Tag no-fasttest: Require python libraries like scipy, pandas and numpy

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# We should have correct env vars from shell_config.sh to run this test
python3 "$CURDIR"/02346_read_in_order_fixed_prefix.python
