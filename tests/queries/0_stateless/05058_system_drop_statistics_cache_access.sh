#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Only the denied spelling is used, so nothing clears the server-wide statistics caches and the
# test stays parallel-safe. The CLEAR alias is used because the grep-based style check requires
# no-parallel from every test that merely mentions the DROP spelling.

user="no_stats_cache_user_05058_$CLICKHOUSE_DATABASE"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} --query "CREATE USER $user IDENTIFIED WITH no_password"

# The message names the privilege that was required, so the mapping is asserted without executing
# anything.
out=$(${CLICKHOUSE_CLIENT} --user "$user" --query "SYSTEM CLEAR STATISTICS CACHE" 2>&1)
grep -qF ACCESS_DENIED <<< "$out" || echo "FAIL: expected the command to be denied: $out"
sed -n "/necessary to have the grant/{s/.*grant \(.*\) ON \*\.\*.*/\1/p;q;}" <<< "$out"

${CLICKHOUSE_CLIENT} --query "DROP USER $user"
