#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_named_scalars=1"
CLICKHOUSE_LOCAL="$CLICKHOUSE_LOCAL --allow_experimental_named_scalars=1"

TEST_DB="${CLICKHOUSE_DATABASE}"
U_CREATE="cv_create_${TEST_DB}"
U_DROP="cv_drop_${TEST_DB}"
U_GET="cv_get_${TEST_DB}"
U_SYS="cv_sys_${TEST_DB}"
U_RO="cv_ro_${TEST_DB}"
U_DEFINER="cv_definer_${TEST_DB}"

cleanup() {
    $CLICKHOUSE_CLIENT --query "DROP NAMED SCALAR IF EXISTS access_cv"
    $CLICKHOUSE_CLIENT --query "DROP NAMED SCALAR IF EXISTS access_user_definer"
    $CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${U_CREATE}, ${U_DROP}, ${U_GET}, ${U_SYS}, ${U_RO}, ${U_DEFINER}"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT --query "CREATE USER ${U_CREATE}, ${U_DROP}, ${U_GET}, ${U_SYS}, ${U_RO}, ${U_DEFINER}"

# Seed a scalar under the default (unrestricted) user. REFRESH EVERY is set
# so that SYSTEM REFRESH below reaches the access check rather than failing
# with NOT_REFRESHABLE; the body uses a non-constant expression because
# REFRESH on a constant SELECT is rejected at install time.
$CLICKHOUSE_CLIENT --query "CREATE NAMED SCALAR access_cv REFRESH EVERY 1 HOUR AS SELECT toUInt32(7) + toUInt32(rand() * 0)"

# --- CREATE_NAMED_SCALAR required for CREATE NAMED SCALAR -------------------------
$CLICKHOUSE_CLIENT --user ${U_CREATE} --query "CREATE NAMED SCALAR access_user AS SELECT 1" 2>&1 \
    | grep -q ACCESS_DENIED && echo "create_without_grant=DENIED" || echo "create_without_grant=ALLOWED"

$CLICKHOUSE_CLIENT --user ${U_CREATE} --query "CREATE NAMED SCALAR access_user_definer DEFINER = ${U_DEFINER} AS SELECT toUInt32(9)" 2>&1 \
    | grep -q ACCESS_DENIED && echo "set_definer_without_create_grant=DENIED" || echo "set_definer_without_create_grant=ALLOWED"

$CLICKHOUSE_CLIENT --query "GRANT CREATE NAMED SCALAR ON *.* TO ${U_CREATE}"
$CLICKHOUSE_CLIENT --user ${U_CREATE} --query "CREATE NAMED SCALAR access_user AS SELECT toUInt32(1)" 2>&1 | head -1
$CLICKHOUSE_CLIENT --user ${U_CREATE} --query "DROP NAMED SCALAR access_user" 2>&1 \
    | grep -q ACCESS_DENIED && echo "drop_without_grant=DENIED" || echo "drop_without_grant=ALLOWED"
$CLICKHOUSE_CLIENT --query "DROP NAMED SCALAR access_user"

# --- DROP_NAMED_SCALAR required for DROP NAMED SCALAR -----------------------------
$CLICKHOUSE_CLIENT --user ${U_DROP} --query "DROP NAMED SCALAR access_cv" 2>&1 \
    | grep -q ACCESS_DENIED && echo "bare_drop=DENIED" || echo "bare_drop=ALLOWED"

# --- getNamedScalar requires getNamedScalar access ------------------------------
$CLICKHOUSE_CLIENT --user ${U_GET} --query "SELECT getNamedScalar('access_cv')" 2>&1 \
    | grep -q ACCESS_DENIED && echo "get_without_grant=DENIED" || echo "get_without_grant=ALLOWED"

$CLICKHOUSE_CLIENT --query "GRANT getNamedScalar ON *.* TO ${U_GET}"
$CLICKHOUSE_CLIENT --user ${U_GET} --query "SELECT getNamedScalar('access_cv')"

# One getNamedScalar grant covers both read functions.
$CLICKHOUSE_CLIENT --user ${U_GET} --query "SELECT getNamedScalarOrDefault('access_cv', toUInt32(0))"
$CLICKHOUSE_CLIENT --user ${U_GET} --query "SELECT getNamedScalarOrDefault('missing', toUInt32(0))"

# system.named_scalars is visible to users with either SHOW_NAMED_SCALARS
# or getNamedScalar; asserting the getNamedScalar branch pins that fallback.
$CLICKHOUSE_CLIENT --user ${U_GET} --query \
    "SELECT count() FROM system.named_scalars WHERE kind = 'local' AND name = 'access_cv'"

# --- SYSTEM REFRESH NAMED SCALAR requires SYSTEM REFRESH NAMED SCALAR ------
$CLICKHOUSE_CLIENT --user ${U_SYS} --query "SYSTEM REFRESH NAMED SCALAR access_cv" 2>&1 \
    | grep -q ACCESS_DENIED && echo "sys_refresh_without_grant=DENIED" || echo "sys_refresh_without_grant=ALLOWED"

$CLICKHOUSE_CLIENT --query "GRANT SYSTEM REFRESH NAMED SCALAR ON *.* TO ${U_SYS}"
$CLICKHOUSE_CLIENT --user ${U_SYS} --query "SYSTEM REFRESH NAMED SCALAR access_cv" 2>&1 | head -1
$CLICKHOUSE_CLIENT --query "GRANT SYSTEM START NAMED SCALAR REFRESHES ON *.* TO ${U_SYS}"
$CLICKHOUSE_CLIENT --query "GRANT SYSTEM STOP NAMED SCALAR REFRESHES ON *.* TO ${U_SYS}"
echo "system_alias_grants=OK"

# --- Readonly profile blocks CREATE NAMED SCALAR ------------------------------
$CLICKHOUSE_CLIENT --query "GRANT CREATE NAMED SCALAR ON *.* TO ${U_RO}"
$CLICKHOUSE_CLIENT --user ${U_RO} --readonly 1 --query "CREATE NAMED SCALAR access_ro AS SELECT 1" 2>&1 \
    | grep -qE "ACCESS_DENIED|READONLY" && echo "readonly_blocks_create=DENIED" || echo "readonly_blocks_create=ALLOWED"

# --- SET DEFINER required for another definer user ------------------------------
$CLICKHOUSE_CLIENT --user ${U_CREATE} --query "CREATE NAMED SCALAR access_user_definer DEFINER = ${U_DEFINER} AS SELECT toUInt32(9)" 2>&1 \
    | grep -q ACCESS_DENIED && echo "set_definer_without_grant=DENIED" || echo "set_definer_without_grant=ALLOWED"

$CLICKHOUSE_CLIENT --query "GRANT SET DEFINER ON ${U_DEFINER} TO ${U_CREATE}"
$CLICKHOUSE_CLIENT --user ${U_CREATE} --query "CREATE NAMED SCALAR access_user_definer DEFINER = ${U_DEFINER} AS SELECT toUInt32(9)"
$CLICKHOUSE_CLIENT --query "SELECT definer = '${U_DEFINER}' FROM system.named_scalars WHERE kind = 'local' AND name = 'access_user_definer'" \
    | grep -q 1 && echo "definer_recorded=OK" || echo "definer_recorded=UNEXPECTED"

# --- last_error tier-gating: the definer's exception text in
#     system.named_scalars.exception is operator-tier; readers with
#     only getNamedScalar must see NULL, not the definer's error text.
SECRET_TBL="redacted_${CLICKHOUSE_TEST_UNIQUE_NAME:-default}"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS default.${SECRET_TBL}"
$CLICKHOUSE_CLIENT --query "CREATE TABLE default.${SECRET_TBL} (x UInt8) ENGINE = Memory"
$CLICKHOUSE_CLIENT --query "INSERT INTO default.${SECRET_TBL} VALUES (1)"
$CLICKHOUSE_CLIENT --query "CREATE NAMED SCALAR access_redacted REFRESH EVERY 36500 DAYS AS SELECT count() FROM default.${SECRET_TBL}"
$CLICKHOUSE_CLIENT --query "DROP TABLE default.${SECRET_TBL}"
$CLICKHOUSE_CLIENT --query "SYSTEM REFRESH NAMED SCALAR access_redacted"
# Wait for the failure to land.
for _ in $(seq 1 30); do
    last=$($CLICKHOUSE_CLIENT --query "SELECT coalesce(exception, '') FROM system.named_scalars WHERE name = 'access_redacted'")
    [ -n "$last" ] && break
    sleep 0.1
done
# Value-tier reader (getNamedScalar only): exception column must be NULL,
# never expose the definer's error text.
$CLICKHOUSE_CLIENT --user ${U_GET} --query \
    "SELECT exception IS NULL FROM system.named_scalars WHERE name='access_redacted'" \
    | tr '\n' ' ' | sed 's/ $//; s/^/value_tier_exception_null=/'
echo
$CLICKHOUSE_CLIENT --user ${U_GET} --query \
    "SELECT exception FROM system.named_scalars WHERE name='access_redacted'" 2>&1 \
    | grep -q "${SECRET_TBL}" && echo "value_tier_leak=YES_BUG" || echo "value_tier_leak=NO_OK"
# Operator-tier reader (SHOW_NAMED_SCALARS): exception is populated.
U_SHOW="u_show_redacted_${CLICKHOUSE_TEST_UNIQUE_NAME:-default}"
$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${U_SHOW}"
$CLICKHOUSE_CLIENT --query "CREATE USER ${U_SHOW} IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT --query "GRANT SHOW NAMED SCALARS ON *.* TO ${U_SHOW}"
$CLICKHOUSE_CLIENT --user ${U_SHOW} --query \
    "SELECT exception FROM system.named_scalars WHERE name='access_redacted'" \
    | grep -q "${SECRET_TBL}" && echo "operator_tier_sees_full=YES_OK" || echo "operator_tier_sees_full=NO_BUG"
$CLICKHOUSE_CLIENT --query "DROP USER ${U_SHOW}"
$CLICKHOUSE_CLIENT --query "DROP NAMED SCALAR access_redacted"
