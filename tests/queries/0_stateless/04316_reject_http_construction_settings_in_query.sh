#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The HTTP query-construction / response-shaping settings (`select`, `filter`, `order`, `sort`,
# `page`, `compression`) are consumed by the HTTP handler before the query is executed. Supplying
# them through an in-query SETTINGS clause has no effect on any protocol, so it is rejected with a
# clear error instead of being silently ignored.

for setting in select filter order sort page compression; do
    # Use a value that is valid for the setting's type so the rejection (not a parse/type error) is
    # what we observe. `page` is numeric; the rest are strings.
    if [ "$setting" = "page" ]; then
        value="1"
    else
        value="'x'"
    fi
    ${CLICKHOUSE_CLIENT} --query "SELECT 1 SETTINGS \`${setting}\` = ${value}" 2>&1 \
        | grep -o -m1 "Setting '${setting}' shapes the HTTP request/response" \
        || echo "FAIL: ${setting} was not rejected"
done

# Settings that DO take effect when set via an in-query SETTINGS clause must keep working.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM (SELECT number FROM numbers(10) SETTINGS limit = 3)"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM (SELECT number FROM numbers(10) SETTINGS offset = 4)"
