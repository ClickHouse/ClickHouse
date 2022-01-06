#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Not sure why fail even in sequential mode. Disabled for now to make some progress.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# We should have correct env vars from shell_config.sh to run this test

#url auth test moved to 02157_url_auth.sh
#python3 "$CURDIR"/02126_url_auth.python

