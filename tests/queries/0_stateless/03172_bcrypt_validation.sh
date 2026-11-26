#!/usr/bin/env bash
# Tags: no-fasttest
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
# Invalid bcrypt hash must be rejected
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} IDENTIFIED WITH bcrypt_hash BY '012345678901234567890123456789012345678901234567890123456789'" 2>&1 | grep -m1 -o 'BAD_ARGUMENTS'
