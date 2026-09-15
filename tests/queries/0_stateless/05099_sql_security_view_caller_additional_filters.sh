#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `StorageInMemoryMetadata::getSQLSecurityOverriddenContext` replays the caller's changed settings
# into the definer's (or, for `SQL SECURITY NONE`, the unrestricted) context, so that the view's
# inner query honours the caller's profile. `additional_table_filters` and `additional_result_filter`
# must not be replayed: they inject expressions that would be evaluated under the elevated identity
# over tables and columns the caller cannot read, and they would override the filters of the
# definer's own profile.

db=${CLICKHOUSE_DATABASE}
invoker="user05099_${CLICKHOUSE_DATABASE}_$RANDOM"
definer="definer05099_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOF
CREATE TABLE $db.secrets05099 (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO $db.secrets05099 VALUES ('${invoker}', 'visible'), ('someone_else', 'HIDDEN');

CREATE USER $invoker;
CREATE USER $definer SETTINGS additional_result_filter = 'owner != \'someone_else\'';
GRANT SELECT ON $db.secrets05099 TO $definer;

-- The views expose only the owner column: the secret column is readable by nobody but the definer.
CREATE VIEW $db.owners_definer05099 DEFINER = $definer SQL SECURITY DEFINER AS SELECT owner FROM $db.secrets05099;
CREATE VIEW $db.owners_none05099 SQL SECURITY NONE AS SELECT owner FROM $db.secrets05099;
CREATE VIEW $db.owners_invoker05099 SQL SECURITY INVOKER AS SELECT owner FROM $db.secrets05099;
GRANT SELECT ON $db.owners_definer05099 TO $invoker;
GRANT SELECT ON $db.owners_none05099 TO $invoker;
GRANT SELECT ON $db.owners_invoker05099 TO $invoker;
EOF

# Quoted for use inside the map literal of the SETTINGS clause.
leak_filter="throwIf(secret = ''HIDDEN'', ''LEAKED'') = 0"

# shellcheck disable=SC2086
for analyzer in 1 0 inline; do
    case "$analyzer" in
        1) label="analyzer"; opts="--enable_analyzer 1" ;;
        0) label="legacy analyzer"; opts="--enable_analyzer 0" ;;
        inline) label="analyzer, inline views"; opts="--enable_analyzer 1 --analyzer_inline_views 1" ;;
    esac

    # A caller-supplied filter on the view's inner table must not be evaluated inside the view.
    # The caller has no access to the inner table at all, so the invoker twin fails on access.
    for view in owners_definer05099 owners_none05099; do
        echo "$view, $label, additional_table_filters on the inner table: $(${CLICKHOUSE_CLIENT} $opts --user "$invoker" --query "SELECT count() FROM $db.$view SETTINGS additional_table_filters = {'$db.secrets05099': '$leak_filter'}" 2>&1 | grep -q -F FUNCTION_THROW_IF_VALUE_IS_NON_ZERO && echo 1 || echo 0) leaks"
    done
    echo "owners_invoker05099, $label, additional_table_filters on the inner table: $(${CLICKHOUSE_CLIENT} $opts --user "$invoker" --query "SELECT count() FROM $db.owners_invoker05099 SETTINGS additional_table_filters = {'$db.secrets05099': '$leak_filter'}" 2>&1 | grep -q -F ACCESS_DENIED && echo 1 || echo 0) access denied"

    # The definer profile's own result filter hides one row; the caller's result filter applies to
    # the caller's own query only and must not replace it inside the view.
    ${CLICKHOUSE_CLIENT} $opts --user "$invoker" --query "SELECT 'owners_definer05099, $label, caller additional_result_filter:', count() FROM $db.owners_definer05099 SETTINGS additional_result_filter = '1'"
    ${CLICKHOUSE_CLIENT} $opts --user "$invoker" --query "SELECT 'owners_definer05099, $label, no caller filter:', count() FROM $db.owners_definer05099"

    # A caller filter on the view itself is honoured, as before.
    ${CLICKHOUSE_CLIENT} $opts --user "$invoker" --query "SELECT 'owners_none05099, $label, additional_table_filters on the view:', count() FROM $db.owners_none05099 SETTINGS additional_table_filters = {'$db.owners_none05099': 'owner = \'someone_else\''}"
    echo "owners_none05099, $label, caller additional_result_filter: $(${CLICKHOUSE_CLIENT} $opts --user "$invoker" --query "SELECT owner FROM $db.owners_none05099 ORDER BY owner SETTINGS additional_result_filter = 'owner = \'someone_else\''" 2>&1)"
done

${CLICKHOUSE_CLIENT} <<EOF
DROP VIEW $db.owners_definer05099;
DROP VIEW $db.owners_none05099;
DROP VIEW $db.owners_invoker05099;
DROP USER $invoker;
DROP USER $definer;
DROP TABLE $db.secrets05099;
EOF
