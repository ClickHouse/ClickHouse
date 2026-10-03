#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Without a quota key, `KEYED BY client_key, user_name` and `KEYED BY client_key, ip_address` fall back to the
# user name and the client address; only `KEYED BY client_key` rejects the query.
for key_type in "client_key, user_name" "client_key, ip_address" "client_key"; do
    query="CREATE QUOTA q KEYED BY ${key_type} FOR INTERVAL 1 YEAR MAX queries = 100 TO default;
           SELECT '${key_type}', quota_key FROM system.quota_usage WHERE quota_name = 'q' ORDER BY duration"
    ${CLICKHOUSE_LOCAL} -q "${query}" 2>&1 | grep -oE "QUOTA_REQUIRES_CLIENT_KEY|^client_key.*"
    # clickhouse-local takes the quota key only as a config override.
    ${CLICKHOUSE_LOCAL} -q "${query}" -- --quota_key=kb
done
