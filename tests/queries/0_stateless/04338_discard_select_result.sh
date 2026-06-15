#!/usr/bin/env bash
# Tags: no-parallel
# ^ no-parallel: creates a settings profile and a user with fixed names.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# discard_select_result: the SELECT runs on real data, but no result is returned
# to the client over any protocol or FORMAT. SHOW/DESCRIBE/EXPLAIN stay unaffected.

echo "--- default off: data is returned ---"
${CLICKHOUSE_CLIENT} --query "SELECT number FROM numbers(3) ORDER BY number"

echo "--- native protocol: no rows for any FORMAT ---"
${CLICKHOUSE_CLIENT} --discard_select_result 1 --query "SELECT number FROM numbers(3) ORDER BY number"
${CLICKHOUSE_CLIENT} --discard_select_result 1 --query "SELECT number FROM numbers(3) ORDER BY number FORMAT JSONEachRow"
${CLICKHOUSE_CLIENT} --discard_select_result 1 --query "SELECT number FROM numbers(3) ORDER BY number FORMAT Values"
echo "native: no rows"

echo "--- native protocol: totals and extremes are suppressed ---"
${CLICKHOUSE_CLIENT} --discard_select_result 1 --extremes 1 --query "SELECT number FROM numbers(3) GROUP BY number WITH TOTALS ORDER BY number"
echo "native: no totals/extremes"

echo "--- HTTP: no rows for any FORMAT ---"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&discard_select_result=1" -d "SELECT number FROM numbers(3) ORDER BY number"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&discard_select_result=1" -d "SELECT number FROM numbers(3) ORDER BY number FORMAT JSONEachRow"
echo "http: no rows"

echo "--- HTTP: totals and extremes are suppressed ---"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&discard_select_result=1&extremes=1" -d "SELECT number FROM numbers(3) GROUP BY number WITH TOTALS ORDER BY number"
echo "http: no totals/extremes"

echo "--- the query really executes on real data (side effects happen) ---"
${CLICKHOUSE_CLIENT} --discard_select_result 1 --query "SELECT throwIf(number = 2, 'pipeline executed') FROM numbers(3)" 2>&1 | grep -o "pipeline executed" | head -n 1

echo "--- SHOW / DESCRIBE / EXPLAIN are unaffected ---"
[[ $(${CLICKHOUSE_CLIENT} --discard_select_result 1 --query "DESCRIBE (SELECT 1 AS x, 'a' AS y)" | wc -l) -eq 2 ]] && echo "describe: ok"
[[ $(${CLICKHOUSE_CLIENT} --discard_select_result 1 --query "SHOW DATABASES" | wc -l) -gt 0 ]] && echo "show: ok"
[[ $(${CLICKHOUSE_CLIENT} --discard_select_result 1 --query "EXPLAIN SELECT 1" | wc -l) -gt 0 ]] && echo "explain: ok"

echo "--- INTO OUTFILE is rejected ---"
${CLICKHOUSE_CLIENT} --discard_select_result 1 --query "SELECT 1 INTO OUTFILE '${CLICKHOUSE_TMP}/04338_out.tsv'" 2>&1 | grep -o "INTO OUTFILE is not allowed when discard_select_result is enabled"

echo "--- pinned non-overridably via a settings profile (CONST) ---"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS user_04338_discard"
${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE IF EXISTS profile_04338_discard"
${CLICKHOUSE_CLIENT} --query "CREATE SETTINGS PROFILE profile_04338_discard SETTINGS discard_select_result = 1 CONST"
${CLICKHOUSE_CLIENT} --query "CREATE USER user_04338_discard SETTINGS PROFILE profile_04338_discard"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON *.* TO user_04338_discard"
echo "pinned user result (expected empty):"
${CLICKHOUSE_CLIENT} --user user_04338_discard --query "SELECT number FROM numbers(3) ORDER BY number"
echo "pinned user override attempt:"
${CLICKHOUSE_CLIENT} --user user_04338_discard --query "SELECT number FROM numbers(3) SETTINGS discard_select_result = 0" 2>&1 | grep -o "Setting discard_select_result should not be changed" | head -n 1
${CLICKHOUSE_CLIENT} --query "DROP USER user_04338_discard"
${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE profile_04338_discard"
