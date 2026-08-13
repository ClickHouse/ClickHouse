#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Creates an entity, then creates a second one from the statement the server emitted for the first,
# and compares the two emitted statements. This is the loop serializeAccessEntity /
# deserializeAccessEntity runs on every access-storage load.
round_trip() {
    local kind="$1" label="$2" clause="$3"
    local a="a_${U}" b="b_${U}"
    local create_kw show_kw

    case "$kind" in
        profile) create_kw="SETTINGS PROFILE"; show_kw="SETTINGS PROFILE" ;;
        user)    create_kw="USER";            show_kw="USER" ;;
        role)    create_kw="ROLE";            show_kw="ROLE" ;;
    esac

    ${CLICKHOUSE_CLIENT} -q "DROP ${create_kw} IF EXISTS ${a}, ${b}"
    ${CLICKHOUSE_CLIENT} -q "CREATE ${create_kw} ${a} SETTINGS ${clause}"

    local emitted_a emitted_b
    emitted_a=$(${CLICKHOUSE_CLIENT} -q "SHOW CREATE ${show_kw} ${a} FORMAT TSVRaw")

    # Feed the server's own output back, under the second name.
    ${CLICKHOUSE_CLIENT} -q "${emitted_a//$a/$b}" 2>/dev/null
    emitted_b=$(${CLICKHOUSE_CLIENT} -q "SHOW CREATE ${show_kw} ${b} FORMAT TSVRaw" 2>/dev/null)

    if [[ -n "$emitted_a" && "$emitted_a" == "${emitted_b//$b/$a}" ]]; then
        echo "${label} round trip OK"
    else
        echo "${label} round trip FAILED"
        echo "  emitted: ${emitted_a}"
        echo "  reparsed: ${emitted_b}"
    fi

    ${CLICKHOUSE_CLIENT} -q "DROP ${create_kw} IF EXISTS ${a}, ${b}"
}

# kind|label|settings clause. Quoted heredoc: no shell expansion, SQL escapes pass through verbatim.
while IFS='|' read -r kind label clause; do
    [[ -z "$kind" ]] && continue
    round_trip "$kind" "$label" "$clause"
done <<'ARMS'
profile|arm01 profile map|http_response_headers = '{\'Content-Type\':\'application/json\'}' CONST
profile|arm02 profile empty map|http_response_headers = '{}' CONST
profile|arm03 additional_table_filters|additional_table_filters = '{\'default.t\':\'x > 0\'}'
user|arm04 user map|http_response_headers = '{\'a\':\'b\'}'
role|arm05 role map|additional_table_filters = '{\'default.t\':\'x > 0\'}'
profile|arm06 min max|http_response_headers = '{\'a\':\'b\'}' MIN '{}' MAX '{\'a\':\'b\'}'
profile|arm07 multi key map|http_response_headers = '{\'Content-Type\':\'application/json\',\'X-A\':\'*\',\'X-B\':\'1\'}' CONST
profile|arm08 hostile value|http_response_headers = '{\'k,1:{}[]()\':\'va,l:ue{}[]()\\\'x\',\'k2\':\'back\\\\slash\'}'
profile|arm09 scalar control|max_memory_usage = 5000000 MIN 4000000 MAX 6000000 CONST
profile|arm10 string valued builtin control|log_comment = '{\'not\':\'a map\'}'
profile|arm11 custom setting control|custom_04902_a = 'plain string'
ARMS

# arm12: ALTER then round trip.
P="alter_${U}"
Q="alter2_${U}"
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${P}, ${Q}"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${P} SETTINGS max_memory_usage = 5000000"
${CLICKHOUSE_CLIENT} -q "ALTER SETTINGS PROFILE ${P} ADD SETTINGS http_response_headers = '{\'a\':\'b\'}' CONST"
EMITTED=$(${CLICKHOUSE_CLIENT} -q "SHOW CREATE SETTINGS PROFILE ${P} FORMAT TSVRaw")
${CLICKHOUSE_CLIENT} -q "${EMITTED//$P/$Q}" 2>/dev/null
REPARSED=$(${CLICKHOUSE_CLIENT} -q "SHOW CREATE SETTINGS PROFILE ${Q} FORMAT TSVRaw" 2>/dev/null)
if [[ -n "$EMITTED" && "$EMITTED" == "${REPARSED//$Q/$P}" ]]; then
    echo "arm12 alter add settings round trip OK"
else
    echo "arm12 alter add settings round trip FAILED"
    echo "  emitted: ${EMITTED}"
    echo "  reparsed: ${REPARSED}"
fi
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${P}, ${Q}"

# arm13: the system table shows the canonical text, and SHOW CREATE now emits that same text.
S="sys_${U}"
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${S}"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${S} SETTINGS http_response_headers = '{\'Content-Type\':\'application/json\'}' CONST"
${CLICKHOUSE_CLIENT} -q "SELECT 'arm13 system table value', value FROM system.settings_profile_elements WHERE profile_name = '${S}' FORMAT TSVRaw"
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${S}"

# arm14: the emitted statement of a map setting is a string literal, not a collection literal.
T="lit_${U}"
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${T}"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${T} SETTINGS http_response_headers = '{\'a\':\'b\'}' CONST"
LIT=$(${CLICKHOUSE_CLIENT} -q "SHOW CREATE SETTINGS PROFILE ${T} FORMAT TSVRaw")
if [[ "$LIT" == *"= '"* && "$LIT" != *"= ["* ]]; then
    echo "arm14 emitted a string literal not a collection literal"
else
    echo "arm14 FAILED: ${LIT}"
fi
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${T}"
