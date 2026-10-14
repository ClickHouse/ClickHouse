#!/usr/bin/env bash
# Tags: no-parallel:misc-caches
# Tag no-parallel: serializes tests that mutate or assert the shared `misc-caches` resource
# (this test issues `SYSTEM CLEAR DNS CACHE`, which is process-wide)

MYHOSTNAME=$(hostname -f)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER1="dns_fail_1_${CLICKHOUSE_TEST_UNIQUE_NAME}"
USER2="dns_fail_2_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${USER1}, ${USER2};
    CREATE USER ${USER1} HOST NAME 'non.existing.host.name', '${MYHOSTNAME}';
    CREATE USER ${USER2} HOST NAME '${MYHOSTNAME}', 'non.existing.host.name';"

${CLICKHOUSE_CLIENT} --query "SELECT 1" --user ${USER1} --host ${MYHOSTNAME}

${CLICKHOUSE_CLIENT} --query "SELECT 2" --user ${USER2} --host ${MYHOSTNAME}

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${USER1}, ${USER2}"

${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR DNS CACHE"
