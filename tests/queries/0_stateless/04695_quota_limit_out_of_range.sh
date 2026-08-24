#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A quota limit of a type with a denominator (`execution_time` is kept in nanoseconds) is scaled with floating
# point arithmetic. A value whose scaled form does not fit into `UInt64` used to be converted with a plain
# `static_cast`, which is undefined behavior, reported by UBSan as
# "1.84467e+19 is outside the range of representable values of type 'unsigned long'".
#
# Quotas are server-global, so the name is suffixed with the (unique) database name to keep the test
# isolated when it runs in parallel with itself (e.g. in the flaky check).

quota="quota_04695_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota}"

${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${quota} FOR INTERVAL 1 day MAX execution_time = 1e19" 2>&1 | grep -o "BAD_ARGUMENTS"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${quota} FOR INTERVAL 1 day MAX execution_time = 18446744073709551616" 2>&1 | grep -o "BAD_ARGUMENTS"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${quota} FOR INTERVAL 1 day MAX execution_time = inf" 2>&1 | grep -o "BAD_ARGUMENTS"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${quota} FOR INTERVAL 1 day MAX execution_time = -1e19" 2>&1 | grep -o "BAD_ARGUMENTS"

${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${quota} FOR INTERVAL 1 day MAX execution_time = 1.5"
${CLICKHOUSE_CLIENT} -q "SELECT max_execution_time FROM system.quota_limits WHERE quota_name = '${quota}'"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA ${quota}"
