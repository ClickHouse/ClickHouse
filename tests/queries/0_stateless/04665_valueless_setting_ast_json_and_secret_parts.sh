#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SETTINGS name` with no value is a shorthand for `= true`, and is rejected for a setting whose type
# is not `Bool`. Two paths used to sidestep that rule.

# 1. The AST JSON round trip dropped `SettingChange::shorthand`, so the `clickhouse_json` dialect
# reconstructed the valueless form as an explicit `name = true`, which is accepted for a setting of
# any type. The formatted round trip must keep the valueless form.
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON(parseQueryToJSON(\$\$SELECT 1 SETTINGS max_threads\$\$))
    = formatQuerySingleLine(\$\$SELECT 1 SETTINGS max_threads\$\$)"
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON(parseQueryToJSON(\$\$SELECT 1 SETTINGS max_threads\$\$))"
$CLICKHOUSE_CLIENT -q "SELECT position(parseQueryToJSON(\$\$SELECT 1 SETTINGS max_threads\$\$), '\"shorthand\":true') > 0"

# A setting given an explicit value is unaffected, and a genuine `Bool` setting still round-trips
# in either form.
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON(parseQueryToJSON(\$\$SELECT 1 SETTINGS max_threads = 4\$\$))"
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON(parseQueryToJSON(\$\$SELECT 1 SETTINGS optimize_move_to_prewhere\$\$))"
$CLICKHOUSE_CLIENT -q "SELECT position(parseQueryToJSON(\$\$SELECT 1 SETTINGS max_threads = 4\$\$), 'shorthand') = 0"

# Executing the JSON payload must be rejected exactly like the SQL form, instead of silently running
# with the setting equal to 1.
JSON=$($CLICKHOUSE_CLIENT -q "SELECT parseQueryToJSON(\$\$SELECT 1 SETTINGS max_threads\$\$) FORMAT TSVRaw")
$CLICKHOUSE_CLIENT --dialect clickhouse_json --allow_experimental_json_ast_dialect 1 -q "$JSON" 2>&1 | grep -o "TYPE_MISMATCH" | head -n 1
# The valueless form of a `Bool` setting still executes.
JSON_BOOL=$($CLICKHOUSE_CLIENT -q "SELECT parseQueryToJSON(\$\$SELECT 1 SETTINGS optimize_move_to_prewhere\$\$) FORMAT TSVRaw")
$CLICKHOUSE_CLIENT --dialect clickhouse_json --allow_experimental_json_ast_dialect 1 -q "$JSON_BOOL"

# A payload may pair the flag with a value the parser would never produce. Deserialization must not
# reject it: that runs before `executeQueryImpl` has an AST to mask with, so the raw JSON text -
# the value included - would go down the unmasked logging path. The value is kept, so the query is
# rejected by the setting's own check, and the formatter still prints it - a valueless setting only
# has its value elided when the value really is `true`, so a formatter can never under-report what a
# query carries. `formatQueryFromJSON` shows secrets, exactly like `formatQuerySingleLine` below;
# what matters is that the masking path used for logging hides the password.
CRAFTED="replaceAll(parseQueryToJSON(\$\$SELECT 1 SETTINGS format_avro_schema_registry_url = 'http://user:pass@localhost'\$\$), '{\"name\":\"format_avro_schema_registry_url\"', '{\"name\":\"format_avro_schema_registry_url\",\"shorthand\":true')"
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON($CRAFTED)"
CRAFTED_JSON=$($CLICKHOUSE_CLIENT -q "SELECT $CRAFTED FORMAT TSVRaw")
# Over HTTP, so the rejection happens server-side and the query reaches `system.query_log`. Rejecting
# the payload while deserializing would have logged the raw JSON text there, password included; what
# must be logged is the masked AST, which omits the value of a valueless setting entirely.
QUERY_ID="04665_crafted_$CLICKHOUSE_DATABASE"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&dialect=clickhouse_json&allow_experimental_json_ast_dialect=1&log_queries=1&query_id=$QUERY_ID" \
    --data-binary "$CRAFTED_JSON" 2>&1 | grep -o "TYPE_MISMATCH" | head -n 1
for _ in {1..60}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    LOGGED=$($CLICKHOUSE_CLIENT -q "SELECT query, position(query, 'pass') = 0 AS no_password FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$QUERY_ID' AND type = 'ExceptionBeforeStart'")
    [ -n "$LOGGED" ] && break
    sleep 0.5
done
echo "$LOGGED"

# 2. `ASTSetQuery::hasSecretParts` read the value of `format_avro_schema_registry_url` as a String.
# `executeQueryImpl` masks the query for logging before any settings validation runs, so a valueless
# setting reached it as Bool `true` and reported `BAD_GET` instead of the intended `TYPE_MISMATCH`.
# The top-level form is validated before that; a subquery reaches the masking path first.
$CLICKHOUSE_CLIENT -q "SELECT * FROM (SELECT 1 SETTINGS format_avro_schema_registry_url)" 2>&1 | grep -o "TYPE_MISMATCH" | head -n 1
$CLICKHOUSE_CLIENT -q "SELECT 1 SETTINGS format_avro_schema_registry_url" 2>&1 | grep -o "TYPE_MISMATCH" | head -n 1

# A setting that really carries a secret is still detected and formatted through the masking path,
# and a value of a type that cannot embed a URI password is not read as a String either.
$CLICKHOUSE_CLIENT -q "SELECT formatQuerySingleLine(\$\$SELECT 1 SETTINGS format_avro_schema_registry_url = 'http://user:pass@localhost'\$\$)"
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON(replaceAll(parseQueryToJSON(\$\$SELECT 1 SETTINGS format_avro_schema_registry_url = 'x'\$\$), '{\"field_type\":\"String\",\"value\":\"x\"}', '{\"field_type\":\"UInt64\",\"value\":1}'))"
