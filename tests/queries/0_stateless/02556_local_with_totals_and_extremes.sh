#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e -o pipefail

# shellcheck disable=SC2086
$CLICKHOUSE_LOCAL -q "SELECT 1, sum(1) with totals format CSV settings extremes=1"

# Both clients skip writing empty data blocks. Totals/extremes must still be
# preceded by the JSON prefix, and finalization must not write a second prefix.
for command in "$CLICKHOUSE_LOCAL" "$CLICKHOUSE_CLIENT"; do
    # Intentional word splitting: the client variables include command-line options.
    # shellcheck disable=SC2086
    $command --output_format_json_quote_64bit_integers=0 --query "
        SELECT number % 2 AS k, count() AS c
        FROM numbers(10)
        WHERE number > 100
        GROUP BY k WITH TOTALS
        FORMAT JSON
    " | python3 -c '
import json, sys
result = json.load(sys.stdin)
assert result["data"] == [], result
assert result["rows"] == 0, result
assert result["totals"] == {"k": 0, "c": 0}, result
print("Empty result with totals: OK")
'

    # OFFSET removes all data rows but preserves the extremes of the input.
    # shellcheck disable=SC2086
    $command --output_format_json_quote_64bit_integers=0 --query "
        SELECT number FROM numbers(10)
        OFFSET 10
        SETTINGS extremes=1
        FORMAT JSON
    " | python3 -c '
import json, sys
result = json.load(sys.stdin)
assert result["data"] == [], result
assert result["rows"] == 0, result
assert result["extremes"] == {"min": {"number": 0}, "max": {"number": 9}}, result
print("Empty result with extremes: OK")
'

    # Check that totals and extremes after nonempty data still work.
    # shellcheck disable=SC2086
    $command --output_format_json_quote_64bit_integers=0 --query "
        SELECT number % 2 AS k, count() AS c
        FROM numbers(10)
        GROUP BY k WITH TOTALS
        ORDER BY k
        SETTINGS extremes=1
        FORMAT JSON
    " | python3 -c '
import json, sys
result = json.load(sys.stdin)
assert result["data"] == [{"k": 0, "c": 5}, {"k": 1, "c": 5}], result
assert result["rows"] == 2, result
assert result["totals"] == {"k": 0, "c": 10}, result
assert result["extremes"] == {"min": {"k": 0, "c": 5}, "max": {"k": 1, "c": 5}}, result
print("Nonempty result with totals and extremes: OK")
'
done
