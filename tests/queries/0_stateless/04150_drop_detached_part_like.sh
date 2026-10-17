#!/usr/bin/env bash
# Tags: no-object-storage, no-shared-merge-tree

# Copies below must use local disk: copying object-storage metadata would alias the blobs.
set -e
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_detached_like_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS storage_policy = 'default'
"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} VALUES (1)"
PART=$(${CLICKHOUSE_CLIENT} --query "
    SELECT name FROM system.parts
    WHERE database = '${CLICKHOUSE_DATABASE}' AND table = '${TABLE}' AND active
    LIMIT 1
")
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DETACH PART '${PART}'"
PART_PATH=$(${CLICKHOUSE_CLIENT} --query "
    SELECT path FROM system.detached_parts
    WHERE database = '${CLICKHOUSE_DATABASE}' AND table = '${TABLE}'
    LIMIT 1
")
DETACHED_DIR=$(dirname "${PART_PATH}")

for name in "ignored_${PART}" "ignored_${PART}_try1" "ignoredx_${PART}" \
    "Ignored_${PART}" "covered-by-broken_${PART}" "attaching_${PART}_try99" "deleting_${PART}_try99" "ignored_malformed"; do
    cp -r "${DETACHED_DIR}/${PART}" "${DETACHED_DIR}/${name}"
done

# Keep active data to verify that even LIKE '%' only affects detached entries.
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} VALUES (99)"

list_parts() {
    ${CLICKHOUSE_CLIENT} --query "
        SELECT name FROM system.detached_parts
        WHERE database = '${CLICKHOUSE_DATABASE}' AND table = '${TABLE}'
        ORDER BY name
    " | sed "s/${PART}/PART/g"
}

# LIKE must preserve the permission gate and reject non-String query parameters.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DROP DETACHED PART LIKE '%' SETTINGS allow_drop_detached = 0; -- { serverError SUPPORT_IS_DISABLED }"
${CLICKHOUSE_CLIENT} --param_pattern=2024-01-01 --query "ALTER TABLE ${TABLE} DROP DETACHED PART LIKE {pattern:Date} SETTINGS allow_drop_detached = 1; -- { serverError BAD_ARGUMENTS }"

# A malformed escape must fail before removing anything.
${CLICKHOUSE_CLIENT} --param_pattern=$'ignored_\\' --query "ALTER TABLE ${TABLE} DROP DETACHED PART LIKE {pattern:String} SETTINGS allow_drop_detached = 1; -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }"

# No-match and empty patterns are no-ops; parameter substitution also works.
${CLICKHOUSE_CLIENT} --param_pattern=missing_% --query "ALTER TABLE ${TABLE} DROP DETACHED PART LIKE {pattern:String} SETTINGS allow_drop_detached = 1"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DROP DETACHED PART LIKE '' SETTINGS allow_drop_detached = 1"
list_parts

# Backslash escapes the literal underscore, so ignoredx_ and Ignored_ must survive.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DROP DETACHED PART LIKE 'ignored\\\\_%' SETTINGS allow_drop_detached = 1"
list_parts

# LIKE '_' matches exactly one character. This drops ignoredx_ without matching Ignored_.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DROP DETACHED PART LIKE 'ignored__%' SETTINGS allow_drop_detached = 1"
list_parts

# Exact-name drops still work after LIKE drops.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DROP DETACHED PART 'covered-by-broken_${PART}' SETTINGS allow_drop_detached = 1"

# Match all remaining entries, but leave directories in use by another operation alone.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DROP DETACHED PART LIKE '%' SETTINGS allow_drop_detached = 1"
list_parts
${CLICKHOUSE_CLIENT} --query "SELECT n FROM ${TABLE} ORDER BY n"

# The two protected fixture directories are not real concurrent operations.
rm -r "${DETACHED_DIR}/attaching_${PART}_try99" "${DETACHED_DIR}/deleting_${PART}_try99"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
