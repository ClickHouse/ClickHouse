#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --hardware_utilization 2>&1 | grep -q "Code: 552. DB::Exception: Unrecognized option '--hardware_utilization'. Maybe you meant \['--hardware-utilization'\]. (UNRECOGNIZED_ARGUMENTS)" && echo 'OK' || echo 'FAIL' ||:
