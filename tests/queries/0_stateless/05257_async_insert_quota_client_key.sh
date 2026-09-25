#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER="u_${CLICKHOUSE_DATABASE}"
QUOTA="q_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${QUOTA}"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t TO ${USER}"

# A strict per-client-key quota: a flush that loses the quota key fails with QUOTA_REQUIRES_CLIENT_KEY.
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${QUOTA} KEYED BY client_key FOR INTERVAL 100 YEAR MAX written bytes = 1000000000 TO ${USER}"

# Two async inserts from the same user but different quota keys. --wait_for_async_insert=1 blocks
# until the flush finishes, so the quota is already charged when we read system.quotas_usage.
${CLICKHOUSE_CLIENT} --user "${USER}" --quota_key tenant_a --async_insert 1 --wait_for_async_insert 1 \
    -q "INSERT INTO t VALUES (1)"
${CLICKHOUSE_CLIENT} --user "${USER}" --quota_key tenant_b --async_insert 1 --wait_for_async_insert 1 \
    -q "INSERT INTO t VALUES (2)"

# Both flushes must have landed.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t"

# Each key must own a separate, non-empty bucket.
${CLICKHOUSE_CLIENT} -q "
SELECT quota_key, sum(written_bytes) > 0
FROM system.quotas_usage
WHERE quota_name = '${QUOTA}' AND quota_key IN ('tenant_a', 'tenant_b')
GROUP BY quota_key
ORDER BY quota_key"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${QUOTA}"
