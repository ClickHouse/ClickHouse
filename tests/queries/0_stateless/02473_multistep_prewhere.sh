#!/usr/bin/env bash
# Tags: long, no-parallel, no-sanitizers, no-flaky-check, no-azure-blob-storage
# ^ The test is heavy

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# We should have correct env vars from shell_config.sh to run this test

python3 "$CURDIR"/02473_multistep_prewhere.python

