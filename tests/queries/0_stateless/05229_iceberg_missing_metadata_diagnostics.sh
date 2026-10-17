#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the fasttest build has no data lake storages.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_iceberg_diagnostics"
rm -rf "${BASE}"

T_EMPTY="${BASE}/empty"
T_UNMATCHED="${BASE}/unmatched"
T_MANY="${BASE}/many"
T_TEMPORARY="${BASE}/temporary"
mkdir -p "${T_EMPTY}/metadata" "${T_UNMATCHED}/metadata" "${T_MANY}/metadata" "${T_TEMPORARY}/metadata"
# A fixed modification time, so the rendering asserted below is the object's own, not a wall clock.
touch -d '2001-09-09 01:46:40 UTC' "${T_UNMATCHED}/metadata/version-hint.text"
for i in $(seq -w 1 12); do touch "${T_MANY}/metadata/unmatched_${i}.text"; done
touch "${T_TEMPORARY}/metadata/00000000-0000-0000-0000-000000000000.metadata.json"
# A second, non-matching object, so `1 candidate` below is a post-filter count over a 2-entry prefix.
touch "${T_TEMPORARY}/metadata/unmatched.text"

EMPTY_ERROR=$(${CLICKHOUSE_CLIENT} -q "SELECT * FROM icebergLocal('${T_EMPTY}/')" 2>&1)
echo "${EMPTY_ERROR}" | grep -qF "The metadata file for Iceberg table with path ${T_EMPTY}/ doesn't exist." \
    && echo "a1: the original sentence is kept"
echo "${EMPTY_ERROR}" | grep -qF "which held 0 entries" \
    && echo "a2: an empty metadata prefix is reported as empty"
echo "${EMPTY_ERROR}" | grep -qE "5 listing attempts over [0-9]+ ms found no \.metadata\.json under .*/metadata" \
    && echo "f: the listing attempts and the time they spanned are reported"

UNMATCHED_ERROR=$(${CLICKHOUSE_CLIENT} -q "SELECT * FROM icebergLocal('${T_UNMATCHED}/')" 2>&1)
echo "${UNMATCHED_ERROR}" | grep -qF "1 entry: /version-hint.text" \
    && echo "b1: an entry the suffix filter dropped is named"
echo "${UNMATCHED_ERROR}" | grep -qF "/version-hint.text (2001-09-09T01:46:40Z)" \
    && echo "b2: its modification time is reported"

MANY_ERROR=$(${CLICKHOUSE_CLIENT} -q "SELECT * FROM icebergLocal('${T_MANY}/')" 2>&1)
echo "${MANY_ERROR}" | grep -qE "12 entries: .*, and 2 more" \
    && echo "c1: a long listing keeps its total and counts the remainder"
echo "c2: entry names printed: $(echo "${MANY_ERROR}" | grep -oE "unmatched_[0-9]{2}\.text" | sort -u | wc -l | tr -d ' ')"

echo "d: retry log lines: $(${CLICKHOUSE_CLIENT} --send_logs_level=debug -q "SELECT * FROM icebergLocal('${T_EMPTY}/')" 2>&1 | grep -c 'returned no usable metadata file')"

# The reported listing is one of the `MAX_LIST_RETRIES` listings, not a fresh one issued to build the message.
echo "e: listing traces: $(${CLICKHOUSE_CLIENT} --send_logs_level=trace -q "SELECT * FROM icebergLocal('${T_EMPTY}/')" 2>&1 | grep -c 'Listed 0 of 0 files')"

${CLICKHOUSE_CLIENT} --send_logs_level=trace -q "SELECT * FROM icebergLocal('${T_UNMATCHED}/')" 2>&1 \
    | grep -qF "Listed 0 of 1 files" \
    && echo "g: the raw and the filtered listing counts are both logged"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM icebergLocal('${T_TEMPORARY}/')" 2>&1 \
    | grep -qE "1 candidate found under .*/metadata, 1 of them temporary commit files: /00000000-0000-0000-0000-000000000000\.metadata\.json" \
    && echo "h: a candidate rejected as a temporary commit file is reported"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM icebergLocal('${T_TEMPORARY}/', SETTINGS iceberg_metadata_table_uuid = '00000000-0000-0000-0000-000000000001')" 2>&1 \
    | grep -qE "and table UUID 00000000000000000000000000000001 doesn't exist\. 1 candidate found under .*/metadata, 1 of them temporary commit files: /00000000-0000-0000-0000-000000000000\.metadata\.json" \
    && echo "h2: candidates are reported when table UUID selection finds none"

rm -rf "${BASE}"
