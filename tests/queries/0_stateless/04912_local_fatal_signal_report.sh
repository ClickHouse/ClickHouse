#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="${CLICKHOUSE_TMP:?}/04912_${CLICKHOUSE_DATABASE:?}"
rm -f "$PREFIX".out "$PREFIX".err

# The first query marks the process ready to be signalled, the second keeps it running until then.
$CLICKHOUSE_LOCAL --query "SELECT 'ready'; SELECT sum(sipHash64(number)) FROM numbers_mt(100000000000)" \
    >"$PREFIX".out 2>"$PREFIX".err &
PID=$!

for _ in {1..600}; do
    grep -q ready "$PREFIX".out 2>/dev/null && break
    sleep 0.1
done

kill -ABRT "$PID" 2>/dev/null
wait "$PID" 2>/dev/null
echo "signalled $?"

grep -c 'Short fault info' "$PREFIX".err
grep -c 'Signal description: Aborted' "$PREFIX".err

rm -f "$PREFIX".out "$PREFIX".err
