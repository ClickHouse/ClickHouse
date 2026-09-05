#!/usr/bin/env bash
# UNKNOWN_ROLE is in the HTTP_NOT_FOUND group of exceptionCodeToHTTPStatus, so a bad
# `role=` query parameter must answer 404 and not fall through to the default 500.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS -o /dev/null -w "%{http_code}\n" \
    "${CLICKHOUSE_URL}&role=04870_role_does_not_exist" --data-binary "SELECT 1"

# Baseline: the same URL without the role parameter answers 200, so a non-200 above
# can only come from the role lookup.
${CLICKHOUSE_CURL} -sS -o /dev/null -w "%{http_code}\n" \
    "${CLICKHOUSE_URL}" --data-binary "SELECT 1"
