#!/usr/bin/env bash
# Regression for a full `ATTACH VIEW <name> (<columns>) AS SELECT ...` of a parameterized view:
# unlike a metadata-only `ATTACH TABLE <name>`, it carries a freshly written definition, so the
# declared column list must be latched exactly like at `CREATE` time - kept when
# `use_declared_schema_for_parameterized_views` is enabled and dropped when it is disabled.
# A full `ATTACH` needs an explicit UUID in `Atomic`/`Replicated` databases, hence a shell test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

attach_view()
{
    local setting=$1
    local name=$2
    local columns=$3
    local uuid
    uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
    $CLICKHOUSE_CLIENT -q "
        SET use_declared_schema_for_parameterized_views = ${setting};
        ATTACH VIEW ${name} UUID '${uuid}' (${columns}) AS
        SELECT number AS n
        FROM numbers({upper_bound:UInt64})" 2>/dev/null # the server warns that a full ATTACH is not recommended
}

echo '-- setting off: the declared schema is not part of the stored definition'
attach_view 0 pv_full_attach_off 'n UInt64'
$CLICKHOUSE_CLIENT -q "SHOW COLUMNS IN pv_full_attach_off"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 'pv_full_attach_off'"

echo '-- setting off: a mismatching declared schema is not enforced either'
attach_view 0 pv_full_attach_off_mismatch 'n UInt64, s String'
$CLICKHOUSE_CLIENT -q "SELECT * FROM pv_full_attach_off_mismatch(upper_bound = 3)"

echo '-- setting on: the declared schema is latched, exposed...'
attach_view 1 pv_full_attach_on 'n UInt64'
$CLICKHOUSE_CLIENT -q "SHOW COLUMNS IN pv_full_attach_on"
$CLICKHOUSE_CLIENT -q "DESCRIBE TABLE pv_full_attach_on"

echo '-- ...and enforced, regardless of the setting value at query time'
$CLICKHOUSE_CLIENT -q "SET use_declared_schema_for_parameterized_views = 0; SELECT * FROM pv_full_attach_on(upper_bound = 3)"
attach_view 1 pv_full_attach_on_mismatch 'n UInt64, s String'
$CLICKHOUSE_CLIENT -q "SET use_declared_schema_for_parameterized_views = 0; SELECT * FROM pv_full_attach_on_mismatch(upper_bound = 3)" 2>&1 | grep -o -m1 'TYPE_MISMATCH'

$CLICKHOUSE_CLIENT -q "
    DROP VIEW pv_full_attach_off;
    DROP VIEW pv_full_attach_off_mismatch;
    DROP VIEW pv_full_attach_on;
    DROP VIEW pv_full_attach_on_mismatch;"
