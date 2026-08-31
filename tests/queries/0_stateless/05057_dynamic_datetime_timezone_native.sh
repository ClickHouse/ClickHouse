#!/usr/bin/env bash
# The FLATTENED Dynamic serialization of the Native format lists its variant types by name, and the
# name of a DateTime that declares no time zone carries none, so such a type has to be built again
# for every read. Each pair below runs in one client invocation, so its second read runs where the
# first one has already built that type.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mkdir -p "${USER_FILES_PATH}"
payload=${CLICKHOUSE_TEST_UNIQUE_NAME}.native

$CLICKHOUSE_CLIENT --output_format_native_use_flattened_dynamic_and_json_serialization 1 \
    --query "SELECT toDateTime(0)::Dynamic AS d,
                    toDateTime(0) AS plain,
                    toDateTime(0, 'Europe/Berlin')::Dynamic AS berlin
             FROM numbers(100) FORMAT Native" > "${USER_FILES_PATH}/${payload}"

# Which thread serves a read is not fixed, so the pairs run several times over. Every read of a
# time zone has to give that zone's rendering, and any other rendering shows up as an extra line.
for _ in {1..8}; do
    $CLICKHOUSE_CLIENT --query "
SELECT 'dynamic tokyo', any(toString(d)) FROM file('${payload}', Native)
    SETTINGS session_timezone = 'Asia/Tokyo', max_threads = 1, schema_inference_use_cache_for_file = 0;
SELECT 'dynamic utc', any(toString(d)) FROM file('${payload}', Native)
    SETTINGS session_timezone = 'UTC', max_threads = 1, schema_inference_use_cache_for_file = 0;

-- \`plain\` is an ordinary DateTime column of the same file, reached through the same call and
-- rendered by the same function, but its type is never looked up by name. It tracks the reading
-- session whichever way the variant above behaves, so it shows that these two time zones do
-- discriminate here.
SELECT 'plain tokyo', any(toString(plain)) FROM file('${payload}', Native)
    SETTINGS session_timezone = 'Asia/Tokyo', max_threads = 1, schema_inference_use_cache_for_file = 0;
SELECT 'plain utc', any(toString(plain)) FROM file('${payload}', Native)
    SETTINGS session_timezone = 'UTC', max_threads = 1, schema_inference_use_cache_for_file = 0;

-- A variant that declares its own time zone keeps it under every reading session.
SELECT 'berlin tokyo', any(toString(berlin)) FROM file('${payload}', Native)
    SETTINGS session_timezone = 'Asia/Tokyo', max_threads = 1, schema_inference_use_cache_for_file = 0;
SELECT 'berlin utc', any(toString(berlin)) FROM file('${payload}', Native)
    SETTINGS session_timezone = 'UTC', max_threads = 1, schema_inference_use_cache_for_file = 0;
"
done | LC_ALL=C sort -u

rm -f "${USER_FILES_PATH:?}/${payload}"
