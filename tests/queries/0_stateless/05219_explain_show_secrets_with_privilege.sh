#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the encryption functions are not available in the fast test build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The pretty plan hides secret function arguments unless the user may display secrets: the server
# setting `display_secrets_in_show_and_select`, the `displaySecretsInShowAndSelect` privilege and the
# session setting `format_display_secrets_in_show_and_select` must all be on. The stateless server has
# the server setting off, so clickhouse-local is used to enable it; the default user there holds every
# privilege. Only the filter lines are printed: the rest of the plan depends on the configuration.

explain_filters() {
    ${CLICKHOUSE_LOCAL} --enable_analyzer=1 --format_display_secrets_in_show_and_select="$1" \
        --query "$2" -- --display_secrets_in_show_and_select=1 | grep -o "Filter column: .*"
}

for show in 0 1; do
    echo "-- format_display_secrets_in_show_and_select = $show"

    echo "-- same-side literal"
    explain_filters "$show" "EXPLAIN PLAN actions = 1 SELECT number FROM numbers(1) WHERE empty(HMAC('sha256', toString(number), 'LITERAL_SECRET'))"

    echo "-- key from a derived table"
    explain_filters "$show" "EXPLAIN PLAN actions = 1 SELECT number FROM (SELECT number, 'DERIVED_SECRET' AS k FROM numbers(1)) AS s WHERE empty(HMAC('sha256', toString(number), s.k))"

    echo "-- key from the opposite JOIN side"
    explain_filters "$show" "EXPLAIN PLAN actions = 1 SELECT n.number FROM numbers(1) AS n INNER JOIN (SELECT 'JOIN_SECRET' AS k) AS s ON HMAC('sha256', toString(n.number), s.k) = ''"

    echo "-- legacy format"
    explain_filters "$show" "EXPLAIN PLAN actions = 1, pretty = 0 SELECT number FROM numbers(1) WHERE empty(HMAC('sha256', toString(number), 'LITERAL_SECRET'))"
done
