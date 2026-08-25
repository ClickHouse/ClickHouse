#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --secuer 2>&1 | grep -q "Code: 552. DB::Exception: Unrecognized option '--secuer'. Maybe you meant \['--secure'\]. (UNRECOGNIZED_ARGUMENTS)" && echo 'OK' || echo 'FAIL' ||:
