#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A definer profile can hide rows through `additional_result_filter`: the view's inner query runs
# at subquery depth 0 in the effective security context, so the filter grows on top of its result.
# `StorageView::canHideRows` must fail closed on it, or a projection-only `SQL SECURITY DEFINER`
# view gets inlined and the invoker's predicate observes the rows the filter hides.

db=${CLICKHOUSE_DATABASE}
invoker="user05043_${CLICKHOUSE_DATABASE}_$RANDOM"
definer="definer05043_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.security_view_arf_secrets (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO $db.security_view_arf_secrets VALUES ('${invoker}', 'visible'), ('someone_else', 'HIDDEN');

CREATE USER $invoker;
CREATE USER $definer SETTINGS additional_result_filter = 'owner != \'someone_else\'';
GRANT SELECT ON $db.security_view_arf_secrets TO $definer;

CREATE VIEW $db.security_view_arf
DEFINER = $definer SQL SECURITY DEFINER
AS SELECT * FROM $db.security_view_arf_secrets;
GRANT SELECT ON $db.security_view_arf TO $invoker;
EOF

for settings in "--enable_analyzer 1" "--enable_analyzer 0" "--enable_analyzer 1 --analyzer_inline_views 1"; do
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $settings --user "$invoker" --query \
        "SELECT * FROM $db.security_view_arf WHERE throwIf(secret = 'HIDDEN', 'LEAKED')" 2>&1 | grep -c -F LEAKED
done

${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.security_view_arf"
${CLICKHOUSE_CLIENT} --query "DROP USER $invoker"
${CLICKHOUSE_CLIENT} --query "DROP USER $definer"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.security_view_arf_secrets"
