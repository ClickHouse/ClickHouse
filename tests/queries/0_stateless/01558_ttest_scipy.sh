#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Python is not so fast

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# We should have correct env vars from shell_config.sh to run this test

python3 "$CURDIR"/01558_ttest_scipy.python
