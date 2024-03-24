#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "-- It works."
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&allow_get_client_http_header=1" -d "SELECT getClientHTTPHeader('Content-Type')"

echo "-- It supports non constant arguments."
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&allow_get_client_http_header=1" -d "SELECT getClientHTTPHeader(arrayJoin(['Content-Type', 'Accept']))"

echo "-- Empty string for non-existent headers."
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&allow_get_client_http_header=1" -d "SELECT getClientHTTPHeader(arrayJoin(['Content-Type', 'Upyachka']))"

echo "-- I can use my own headers."
${CLICKHOUSE_CURL} -H 'Upyachka: wtf' "${CLICKHOUSE_URL}&allow_get_client_http_header=1" -d "SELECT getClientHTTPHeader('Upyachka')"

echo "-- Some headers cannot be obtained."
${CLICKHOUSE_CURL} -H 'X-ClickHouse-WTF: Secret' "${CLICKHOUSE_URL}&allow_get_client_http_header=1" -d "SELECT getClientHTTPHeader('X-ClickHouse-WTF')"
${CLICKHOUSE_CURL} -H 'Authentication: Secret' "${CLICKHOUSE_URL}&allow_get_client_http_header=1" -d "SELECT getClientHTTPHeader('Authentication')"

echo "-- The setting matters."
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&allow_get_client_http_header=0" -d "SELECT getClientHTTPHeader(arrayJoin(['Content-Type', 'Accept']))" | grep -o -F 'FUNCTION_NOT_ALLOWED'

echo "-- The setting is not enabled by default."
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" -d "SELECT getClientHTTPHeader(arrayJoin(['Content-Type', 'Accept']))" | grep -o -F 'FUNCTION_NOT_ALLOWED'

echo "-- Are headers case-sentitive?"
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&allow_get_client_http_header=1" -d "SELECT getClientHTTPHeader(arrayJoin(['Content-Type', 'content-type']))"

echo "-- Using it from non-HTTP does not make sense."
${CLICKHOUSE_CLIENT} --allow_get_client_http_header true --query "SELECT getClientHTTPHeader('Host')"
${CLICKHOUSE_LOCAL} --allow_get_client_http_header true --query "SELECT getClientHTTPHeader('Host')"

echo "-- Does it work for distributed queries? (not yet, but maybe it will be needed later)"
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&allow_get_client_http_header=1&prefer_localhost_replica=0" -d "SELECT getClientHTTPHeader(name) FROM clusterAllReplicas('test_cluster_one_shard_two_replicas', view(SELECT 'Content-Type' AS name))"
