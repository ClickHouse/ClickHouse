#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"
CLEANUP=""

# Creates an entity, then creates a second one from the statement the server emitted for the first,
# and compares the two emitted statements. This is the display route, via SHOW CREATE; arm15 covers
# the persistence route, where serializeAccessEntity and deserializeAccessEntity run.
#
# Each half is one client invocation: statements that produce no output share it with the single
# SHOW CREATE whose one line is the result, so an arm costs two processes rather than seven.
round_trip() {
    local n="$1" kind="$2" label="$3" clause="$4" pin_setting="$5"
    local a="a${n}_${U}" b="b${n}_${U}"
    local create_kw owner_col

    case "$kind" in
        profile) create_kw="SETTINGS PROFILE"; owner_col="profile_name" ;;
        user)    create_kw="USER";             owner_col="user_name" ;;
        role)    create_kw="ROLE";             owner_col="role_name" ;;
    esac
    CLEANUP="${CLEANUP} DROP ${create_kw} IF EXISTS ${a}, ${b};"

    local emitted_a emitted_b reparsed pin_query=""
    emitted_a=$(${CLICKHOUSE_CLIENT} -q "
        DROP ${create_kw} IF EXISTS ${a};
        CREATE ${create_kw} ${a} SETTINGS ${clause};
        SHOW CREATE ${create_kw} ${a} FORMAT TSVRaw")

    # Comparing the two emitted statements only proves the serializer agrees with itself, so for the
    # arms whose value carries structure the surviving value is pinned in .reference as well. It is
    # the second line of the same reply.
    if [[ -n "$pin_setting" ]]; then
        pin_query="SELECT '${label} reparsed', value FROM system.settings_profile_elements WHERE ${owner_col} = '${b}' AND setting_name = '${pin_setting}' FORMAT TSVRaw;"
    fi

    # Feed the server's own output back, under the second name. A server that cannot reparse what it
    # emitted fails here, which is what the arm is for, so these errors are expected.
    reparsed=$(${CLICKHOUSE_CLIENT} --ignore-error -q "
        DROP ${create_kw} IF EXISTS ${b};
        ${emitted_a//$a/$b};
        SHOW CREATE ${create_kw} ${b} FORMAT TSVRaw;
        ${pin_query}" 2>/dev/null)
    emitted_b=$(sed -n 1p <<< "$reparsed")

    if [[ -n "$emitted_a" && "$emitted_a" == "${emitted_b//$b/$a}" ]]; then
        echo "${label} round trip OK"
    else
        echo "${label} round trip FAILED"
        echo "  emitted: ${emitted_a}"
        echo "  reparsed: ${emitted_b}"
    fi

    [[ -n "$pin_setting" ]] && sed -n 2p <<< "$reparsed"
}

# kind|label|settings clause|setting whose reparsed value is pinned (empty = shape check only).
# Quoted heredoc: no shell expansion, SQL escapes pass through verbatim.
N=0
while IFS='|' read -r kind label clause pin; do
    [[ -z "$kind" ]] && continue
    N=$((N + 1))
    round_trip "$(printf '%02d' "$N")" "$kind" "$label" "$clause" "$pin"
done <<'ARMS'
profile|arm01 profile map|http_response_headers = '{\'Content-Type\':\'application/json\'}' CONST|http_response_headers
profile|arm02 profile empty map|http_response_headers = '{}' CONST|
profile|arm03 additional_table_filters|additional_table_filters = '{\'default.t\':\'x > 0\'}'|
user|arm04 user map|http_response_headers = '{\'a\':\'b\'}'|
role|arm05 role map|additional_table_filters = '{\'default.t\':\'x > 0\'}'|
profile|arm06 min max|http_response_headers = '{\'a\':\'b\'}' MIN '{}' MAX '{\'a\':\'b\'}'|
profile|arm07 multi key map|http_response_headers = '{\'Content-Type\':\'application/json\',\'X-A\':\'*\',\'X-B\':\'1\'}' CONST|http_response_headers
profile|arm08 hostile value|http_response_headers = '{\'k,1:{}[]()\':\'va,l:ue{}[]()\\\'x\',\'k2\':\'back\\\\slash\'}'|http_response_headers
profile|arm09 scalar control|max_memory_usage = 5000000 MIN 4000000 MAX 6000000 CONST|
profile|arm10 string valued builtin control|log_comment = '{\'not\':\'a map\'}'|
profile|arm11 custom setting control|custom_04902_a = 'plain string'|
ARMS

# arm12: ALTER then round trip.
P="alter_${U}"
Q="alter2_${U}"
EMITTED=$(${CLICKHOUSE_CLIENT} -q "
    DROP SETTINGS PROFILE IF EXISTS ${P};
    CREATE SETTINGS PROFILE ${P} SETTINGS max_memory_usage = 5000000;
    ALTER SETTINGS PROFILE ${P} ADD SETTINGS http_response_headers = '{\'a\':\'b\'}' CONST;
    SHOW CREATE SETTINGS PROFILE ${P} FORMAT TSVRaw")
REPARSED=$(${CLICKHOUSE_CLIENT} --ignore-error -q "
    DROP SETTINGS PROFILE IF EXISTS ${Q};
    ${EMITTED//$P/$Q};
    SHOW CREATE SETTINGS PROFILE ${Q} FORMAT TSVRaw" 2>/dev/null)
if [[ -n "$EMITTED" && "$EMITTED" == "${REPARSED//$Q/$P}" ]]; then
    echo "arm12 alter add settings round trip OK"
else
    echo "arm12 alter add settings round trip FAILED"
    echo "  emitted: ${EMITTED}"
    echo "  reparsed: ${REPARSED}"
fi

# arm13: the system table shows the canonical text, and SHOW CREATE now emits that same text.
S="sys_${U}"
${CLICKHOUSE_CLIENT} -q "
    DROP SETTINGS PROFILE IF EXISTS ${S};
    CREATE SETTINGS PROFILE ${S} SETTINGS http_response_headers = '{\'Content-Type\':\'application/json\'}' CONST;
    SELECT 'arm13 system table value', value FROM system.settings_profile_elements WHERE profile_name = '${S}' FORMAT TSVRaw"

# arm14: the emitted statement of a map setting is a string literal, not a collection literal.
T="lit_${U}"
LIT=$(${CLICKHOUSE_CLIENT} -q "
    DROP SETTINGS PROFILE IF EXISTS ${T};
    CREATE SETTINGS PROFILE ${T} SETTINGS http_response_headers = '{\'a\':\'b\'}' CONST;
    SHOW CREATE SETTINGS PROFILE ${T} FORMAT TSVRaw")
if [[ "$LIT" == *"= '"* && "$LIT" != *"= ["* ]]; then
    echo "arm14 emitted a string literal not a collection literal"
else
    echo "arm14 FAILED: ${LIT}"
fi

# arm16: only a Map is rewritten. A scalar keeps its bare literal, so widening the type check to
# every builtin setting would emit max_memory_usage = '5000000' and change the stored type.
V="num_${U}"
NUM=$(${CLICKHOUSE_CLIENT} -q "
    DROP SETTINGS PROFILE IF EXISTS ${V};
    CREATE SETTINGS PROFILE ${V} SETTINGS max_memory_usage = 5000000 MIN 4000000 MAX 6000000 CONST;
    SHOW CREATE SETTINGS PROFILE ${V} FORMAT TSVRaw")
echo "arm16 scalar literal stays bare ${NUM#*SETTINGS max_memory_usage}"

${CLICKHOUSE_CLIENT} -q "${CLEANUP} DROP SETTINGS PROFILE IF EXISTS ${P}, ${Q}, ${S}, ${T}, ${V};"

# arm15: the on-disk form. The arms above use SHOW CREATE, which takes the display route; the
# stored <uuid>.sql file is written by the attach route, and a second process has to parse it back
# through deserializeAccessEntity before any query can see the entity.
D="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.ondisk"
rm -rf "$D"
${CLICKHOUSE_LOCAL} --path "$D" -q "CREATE SETTINGS PROFILE ondisk SETTINGS http_response_headers = '{\'X-A\':\'1\',\'X-B\':\'2\'}' MIN '{}' MAX '{\'X-A\':\'1\',\'X-B\':\'2\'}'" -- --max_server_memory_usage=8G --memory_worker_use_cgroup=0
${CLICKHOUSE_LOCAL} --path "$D" -q "SELECT 'arm15 on disk', value, min, max FROM system.settings_profile_elements WHERE profile_name = 'ondisk' FORMAT TSVRaw" -- --max_server_memory_usage=8G --memory_worker_use_cgroup=0
rm -rf "$D"
