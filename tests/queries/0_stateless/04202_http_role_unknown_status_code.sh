#!/usr/bin/env bash
# Test: exercises the HTTP status-code mapping for UNKNOWN_ROLE thrown from the HTTP `role=`
# query parameter introduced by PR #62669. The PR's own test 03096 only `grep`s the response
# BODY for "Code: 511" / "UNKNOWN_ROLE" — it never checks the actual HTTP response status.
# A regression in `exceptionCodeToHTTPStatus` that dropped the new mapping
# `UNKNOWN_ROLE -> HTTP_NOT_FOUND` would silently fall through to the default 500
# (`HTTP_INTERNAL_SERVER_ERROR`), breaking REST clients that rely on standard codes for
# retry/error-handling logic. Adding this test guards that contract explicitly.
# Covers: src/Server/HTTP/exceptionCodeToHTTPStatus.cpp:99-108 — the
#         `exception_code == ErrorCodes::UNKNOWN_ROLE` branch in the HTTP_NOT_FOUND group.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# UNKNOWN_ROLE: pick a role name that does not exist => HTTP 404 expected.
# This works without any pre-existing role, so the test does not depend on writable user storage.
echo "### UNKNOWN_ROLE returns HTTP 404"
${CLICKHOUSE_CURL} -sS -o /dev/null -w "%{http_code}\n" \
    "${CLICKHOUSE_URL}&role=04202_role_does_not_exist" \
    --data-binary "SELECT 1"

# Sanity: a request without role param succeeds with 200, proving the URL is otherwise valid.
echo "### Without role param HTTP 200"
${CLICKHOUSE_CURL} -sS -o /dev/null -w "%{http_code}\n" \
    "${CLICKHOUSE_URL}" \
    --data-binary "SELECT 1"
