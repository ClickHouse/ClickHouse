#!/usr/bin/env bash
# Tags: no-random-settings

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# default should send profile events without any explicit setting.
${CLICKHOUSE_CLIENT} -q 'SELECT 1' --profile-events-delay-ms=-1 --print-profile-events 2>&1 | grep -q "Query" && echo "default OK" 
# enabled with explicit setting.
${CLICKHOUSE_CLIENT} -q 'SELECT 1 SETTINGS send_profile_events=1' --profile-events-delay-ms=-1 --print-profile-events 2>&1 | grep -q "Query" && echo "enabled OK" 

# disabled with explicit setting.
${CLICKHOUSE_CLIENT} -q 'SELECT 1 SETTINGS send_profile_events=0' --profile-events-delay-ms=-1 --print-profile-events 2>&1 | grep -v "Query" | grep -q "1" && echo "disabled OK" 

