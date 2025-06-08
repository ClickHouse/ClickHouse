#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# We will check that the server's exit code corresponds to the exception code if it was terminated after exception.
# In this example, we provide an invalid path to the server's config, ignore its logs and check the exit code.
# The exception code is 76 = CANNOT_OPEN_FILE, so the exit code will be 76 % 256.

${CLICKHOUSE_SERVER_BINARY} -- --path /dev/null 2>/dev/null; [[ "$?" == "$((76 % 256))" ]] && echo 'Ok' || echo 'Fail'
