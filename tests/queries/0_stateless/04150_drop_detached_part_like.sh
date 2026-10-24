#!/usr/bin/env bash

set -e
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_detached_like_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (n UInt64) ENGINE = MergeTree PARTITION BY n ORDER BY n
"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES ${TABLE}"

for v in 1 10 11 20 30; do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} VALUES (${v})"
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DETACH PARTITION ${v}"
done

# Keep active data to verify that even LIKE '%' only affects detached entries.
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} VALUES (99)"

list_parts() {
    ${CLICKHOUSE_CLIENT} --query "
        SELECT name FROM system.detached_parts
        WHERE database = '${CLICKHOUSE_DATABASE}' AND table = '${TABLE}'
        ORDER BY name
    "
}

# LIKE must preserve the permission gate and reject non-String query parameters.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DROP DETACHED PART LIKE '%' SETTINGS allow_drop_detached = 0; -- { serverError SUPPORT_IS_DISABLED }"
${CLICKHOUSE_CLIENT} --param_pattern=2024-01-01 --query "ALTER TABLE ${TABLE} DROP DETACHED PART LIKE {pattern:Date} SETTINGS allow_drop_detached = 1; -- { serverError BAD_ARGUMENTS }"

# A malformed escape must fail before removing anything.
${CLICKHOUSE_CLIENT} --param_pattern=$'1_\\' --query "ALTER TABLE ${TABLE} DROP DETACHED PART LIKE {pattern:String} SETTINGS allow_drop_detached = 1; -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }"

# No-match and empty patterns are no-ops; parameter substitution also works.
${CLICKHOUSE_CLIENT} --param_pattern=missing_% --query "ALTER TABLE ${TABLE} DROP DETACHED PART LIKE {pattern:String} SETTINGS allow_drop_detached = 1"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DROP DETACHED PART LIKE '' SETTINGS allow_drop_detached = 1"
list_parts

# Backslash escapes the literal underscore, so only 1_1_1_0 is dropped while 10_2_2_0 and 11_3_3_0 survive.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DROP DETACHED PART LIKE '1\\\\_%' SETTINGS allow_drop_detached = 1"
list_parts

# LIKE '_' matches exactly one character, dropping 10_2_2_0 and 11_3_3_0 while leaving 20_4_4_0 and 30_5_5_0.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DROP DETACHED PART LIKE '1_\\\\_%' SETTINGS allow_drop_detached = 1"
list_parts

# Exact-name drops still work after LIKE drops.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DROP DETACHED PART '20_4_4_0' SETTINGS allow_drop_detached = 1"
list_parts

# Match all remaining detached entries while leaving active data untouched.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DROP DETACHED PART LIKE '%' SETTINGS allow_drop_detached = 1"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.detached_parts WHERE database = '${CLICKHOUSE_DATABASE}' AND table = '${TABLE}'"
${CLICKHOUSE_CLIENT} --query "SELECT n FROM ${TABLE} ORDER BY n"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
