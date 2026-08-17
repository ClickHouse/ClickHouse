#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A user can be governed by several quotas at once (e.g. one keyed by IP address and another by
# normalized query hash), and all of them are enforced. `SHOW CREATE QUOTA` without an explicit name
# (the "current quota" form) must therefore list every quota currently governing the user, not an
# arbitrary single one. Previously it routed through the single-valued current-quota path and showed
# only whichever quota happened to be returned first from the unordered set of governing quotas, which
# was inconsistent with enforcement.
#
# Quotas and users are server-global, so the names are suffixed with the (unique) database name to
# keep the test isolated when it runs in parallel with itself (e.g. in the flaky check).

user="u_04402_${CLICKHOUSE_DATABASE}"
quota_hash="q_04402_hash_${CLICKHOUSE_DATABASE}"
quota_user="q_04402_user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota_hash}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota_user}"

${CLICKHOUSE_CLIENT} -q "CREATE USER ${user}"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW QUOTAS ON *.* TO ${user}"

# Two quotas with different key types, both assigned to the same user.
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${quota_hash} KEYED BY normalized_query_hash FOR INTERVAL 100 YEAR MAX query_selects = 2 TO ${user}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${quota_user} KEYED BY user_name FOR INTERVAL 100 YEAR MAX query_inserts = 2 TO ${user}"

echo "--- SHOW CREATE QUOTA lists every quota currently governing the user (expects 2) ---"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SHOW CREATE QUOTA" | grep -c -E "(${quota_hash}|${quota_user})"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota_hash}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota_user}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
