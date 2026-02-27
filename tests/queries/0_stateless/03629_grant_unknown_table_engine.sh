#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

trap ' ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS user_${CLICKHOUSE_DATABASE}" ' EXIT

${CLICKHOUSE_CLIENT} -q "CREATE USER IF NOT EXISTS user_${CLICKHOUSE_DATABASE}"

out1="$(${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON RandomName34343 TO user_${CLICKHOUSE_DATABASE}" 2>&1)"
echo "$out1" | grep -om1 "Unknown table engine"
s1=$?

# Happy case: known engine should succeed and print nothing relevant
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON MergeTree TO user_${CLICKHOUSE_DATABASE}"

out2="$(${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON AnotherUnknownEngine_${CLICKHOUSE_DATABASE} TO user_${CLICKHOUSE_DATABASE}" 2>&1)"
echo "$out2" | grep -om1 "Unknown table engine"
s2=$?

[ $s1 -eq 0 ] && [ $s2 -eq 0 ] || exit 1