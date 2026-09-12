#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `prefer_column_name_to_alias` decides whether an identifier of the view's inner query binds to a
# select-list alias or to a source column of the same name. For the view below, `leak` is the source
# column `public` when the setting is enabled, and the aliased `secret` when it is not - so the
# setting decides which source column each view column exposes.
#
# The inner query of a `SQL SECURITY DEFINER` view runs with the definer's profile, and the setting
# can come from there alone, without any `SETTINGS` clause in the view's own AST. Inlining the inner
# query into the invoker's query - which the projection-only path of a barrier view still allows -
# must not change that binding: the inner query is always analyzed with the view's effective
# context, while the invoker's own `SELECT` keeps its own settings. This pins that the definer's
# profile decides, on every analyzer path, so a caller's default cannot expose `secret` as `leak`.

db=${CLICKHOUSE_DATABASE}
definer="definer05143_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOSQL
CREATE TABLE $db.alias_secrets (public String, secret String) ENGINE = MergeTree ORDER BY public;
INSERT INTO $db.alias_secrets VALUES ('public_value', 'HIDDEN_value');

CREATE USER $definer SETTINGS prefer_column_name_to_alias = 1;
GRANT SELECT ON $db.alias_secrets TO $definer;

CREATE VIEW $db.alias_definer_view DEFINER = $definer SQL SECURITY DEFINER
AS SELECT secret AS public, public AS leak FROM $db.alias_secrets;

CREATE VIEW $db.alias_invoker_view SQL SECURITY INVOKER
AS SELECT secret AS public, public AS leak FROM $db.alias_secrets;
EOSQL

for path in "--enable_analyzer 0" "--enable_analyzer 1" "--enable_analyzer 1 --analyzer_inline_views 1"; do
    echo "--- $path ---"
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $path --query "SELECT leak FROM $db.alias_definer_view"
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $path --query \
        "SELECT leak FROM $db.alias_definer_view WHERE throwIf(leak LIKE 'HIDDEN%', 'LEAKED') = 0" 2>&1 |
        grep -q FUNCTION_THROW_IF_VALUE_IS_NON_ZERO && echo "leaked: 1" || echo "leaked: 0"
    # The control: without the definer's profile the same body binds `leak` to the alias, so the
    # value above is not the one the caller's own settings would produce.
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} $path --query "SELECT leak FROM $db.alias_invoker_view"
done

${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.alias_definer_view, $db.alias_invoker_view"
${CLICKHOUSE_CLIENT} --query "DROP USER $definer"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.alias_secrets"
