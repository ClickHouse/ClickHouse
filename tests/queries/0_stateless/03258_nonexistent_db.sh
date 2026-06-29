#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The timeout is only a safety net against a genuinely stuck client.
# The client reports `UNKNOWN_DATABASE` within milliseconds on a healthy run,
# but under `amd_llvm_coverage` source-based coverage instrumentation the startup
# can occasionally exceed 5 s under parallel test load. Raising to 60 s keeps
# the test fast on happy paths and short enough to catch a real hang, while
# preserving coverage of this client code path.
timeout 60 ${CLICKHOUSE_CLIENT_BINARY} --database "nonexistent" 2>&1 | grep -o "UNKNOWN_DATABASE" && echo "OK" || echo "FAIL"
