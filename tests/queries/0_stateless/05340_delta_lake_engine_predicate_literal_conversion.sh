#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test.
# Tag no-msan: delta-kernel-rs is not built with MSan.

# A comparison of a Delta column with a constant of another type must prune files exactly as the
# comparison evaluates: rows read with the engine predicate on must equal the rows read with it off.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

ROOT="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_delta_literal"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

MANIFEST="${ROOT}/manifest.tsv"
mkdir -p "${ROOT}"
: > "${MANIFEST}"
FIXTURE_QUERIES=""
declare -A TABLE_COLUMN TABLE_FILES

# make_table <table> <column> <ClickHouse type> <Delta type> <cdf 0|1> <file>...
# Each file is a JSON array of its values, which is also a valid ClickHouse array literal once
# its double quotes become single quotes.
make_table() {
    local table=$1 column=$2 type=$3 delta_type=$4 cdf=$5
    shift 5
    mkdir -p "${ROOT}/${table}/_delta_log"
    TABLE_COLUMN[$table]="${column}"
    TABLE_FILES[$table]=$#
    local i=0 values
    for values in "$@"; do
        i=$((i + 1))
        FIXTURE_QUERIES+="
            INSERT INTO FUNCTION file('${ROOT}/${table}/f${i}.parquet', Parquet, '${column} ${type}')
            SELECT CAST(arrayJoin(${values//\"/\'}), '${type}');"
        printf '%s\t%s\t%s\t%s\t%s\tf%s.parquet\t%s\n' "${table}" "${column}" "${delta_type}" \
            "$([[ "${type}" == Nullable* ]] && echo 1 || echo 0)" "${cdf}" "${i}" "${values}" >> "${MANIFEST}"
    done
}

# Writes one commit per table: protocol, metaData, then an add action with min/max stats per file.
write_logs() {
    python3 - "${ROOT}" "${MANIFEST}" <<'EOF'
import json, os, sys

root, manifest = sys.argv[1], sys.argv[2]
tables = {}
for line in open(manifest):
    table, column, delta_type, nullable, cdf, path, values = line.rstrip("\n").split("\t")
    tables.setdefault(table, (column, delta_type, nullable == "1", cdf == "1", []))[4].append((path, json.loads(values)))

for table, (column, delta_type, nullable, cdf, files) in tables.items():
    schema = {"type": "struct", "fields": [{"name": column, "type": delta_type, "nullable": nullable, "metadata": {}}]}
    actions = [
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 4 if cdf else 2}},
        {"metaData": {"id": table, "format": {"provider": "parquet", "options": {}},
                      "schemaString": json.dumps(schema), "partitionColumns": [],
                      "configuration": {"delta.enableChangeDataFeed": "true"} if cdf else {},
                      "createdTime": 1700000000000}},
    ]
    for path, values in files:
        stats = {"numRecords": len(values), "minValues": {column: min(values)},
                 "maxValues": {column: max(values)}, "nullCount": {column: 0}}
        actions.append({"add": {"path": path, "partitionValues": {},
                                "size": os.path.getsize(os.path.join(root, table, path)),
                                "modificationTime": 1700000000000, "dataChange": True,
                                "stats": json.dumps(stats)}})
    with open(os.path.join(root, table, "_delta_log", "00000000000000000000.json"), "w") as log:
        for action in actions:
            log.write(json.dumps(action) + "\n")
EOF
}

CHECK_TABLES=() CHECK_TZS=() CHECK_WHERES=()
declare -A OFF_ROWS OFF_FILES ON_ROWS ON_FILES

# check <table> <session time zone> <where>
check() {
    CHECK_TABLES+=("$1")
    CHECK_TZS+=("$2")
    CHECK_WHERES+=("$3")
}

# The change data feed applies the engine predicate whatever `delta_lake_enable_engine_predicate` says.
is_cdf() { [[ "$1" == cdf ]]; }

# Runs every check of one (mode, time zone) in one launch; each data query is followed by the
# cumulative `DeltaLakeScannedFiles`, whose consecutive differences are the per-query file counts.
run_batch() {
    local enabled=$1 tz=$2
    local i batch="" table settings
    local -a ids=()
    for i in "${!CHECK_TABLES[@]}"; do
        [[ "${CHECK_TZS[$i]}" == "${tz}" ]] || continue
        ids+=("$i")
        table="${CHECK_TABLES[$i]}"
        settings=""
        is_cdf "${table}" && settings=" SETTINGS delta_lake_snapshot_start_version = 0"
        batch+="SELECT arraySort(groupArray(toString(${TABLE_COLUMN[$table]}))) FROM deltaLakeLocal('${ROOT}/${table}')
                WHERE ${CHECK_WHERES[$i]}${settings};
            SELECT sumIf(value, event = 'DeltaLakeScannedFiles') FROM system.events;"
    done
    local out
    out=$(${CLICKHOUSE_LOCAL} --session_timezone="${tz}" --delta_lake_enable_engine_predicate="${enabled}" \
        --delta_lake_throw_on_engine_predicate_error=1 \
        --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 \
        --input_format_parquet_bloom_filter_push_down=0 --input_format_parquet_dictionary_filter_push_down=0 \
        --query "${batch}")
    local -a lines
    mapfile -t lines <<< "${out}"
    if [[ "${#lines[@]}" != "$(( 2 * ${#ids[@]} ))" ]]; then
        printf 'Unexpected output of the %s batch (engine predicate %s):\n%s\n' "${tz}" "${enabled}" "${out}" >&2
        return 1
    fi
    local j previous=0
    for j in "${!ids[@]}"; do
        i="${ids[$j]}"
        if [[ "${enabled}" == 1 ]]; then
            ON_ROWS[$i]="${lines[2 * j]}"
            ON_FILES[$i]=$(( lines[2 * j + 1] - previous ))
        else
            OFF_ROWS[$i]="${lines[2 * j]}"
            OFF_FILES[$i]=$(( lines[2 * j + 1] - previous ))
        fi
        previous="${lines[2 * j + 1]}"
    done
}

run_all() {
    ${CLICKHOUSE_LOCAL} --query "${FIXTURE_QUERIES}"
    write_logs
    local tz enabled i table
    for tz in UTC Pacific/Kiritimati; do
        for enabled in 0 1; do
            run_batch "${enabled}" "${tz}"
        done
    done
    for i in "${!CHECK_TABLES[@]}"; do
        table="${CHECK_TABLES[$i]}"
        printf '%s [%s]: %s\n' "${table}" "${CHECK_TZS[$i]}" "${CHECK_WHERES[$i]}"
        diff -u <(printf '%s\n' "${OFF_ROWS[$i]}") <(printf '%s\n' "${ON_ROWS[$i]}") || true
        printf '%s\n' "${ON_ROWS[$i]}"
        # The change data feed is pinned by its rows only.
        is_cdf "${table}" && continue
        if [[ "${OFF_FILES[$i]}" != "${TABLE_FILES[$table]}" ]]; then
            printf 'Unexpected unpruned file count %s for: %s\n' "${OFF_FILES[$i]}" "${CHECK_WHERES[$i]}" >&2
            return 1
        fi
        printf '%s\n' "${ON_FILES[$i]}"
    done
}

DT="toDateTime('2026-01-01 12:00:00', 'UTC')"
TOKYO_MIDNIGHT="toDateTime('2026-01-01 00:00:00', 'Asia/Tokyo')"

make_table dates d Date32 date 0 '["2025-12-31"]' '["2026-01-01"]' '["2026-01-02"]'
for where in "d < ${DT}" "d <= ${DT}" "d > ${DT}" "d >= ${DT}" "d = ${DT}" \
    "d != toDateTime('2026-01-01 00:00:01', 'UTC')" "${DT} > d" "${DT} < d" "${DT} >= d" \
    "toDateTime64('2026-01-01 00:00:00.5', 3, 'UTC') <= d" "d < toDateTime64('2026-01-01 00:00:00.5', 3, 'UTC')" \
    "d < toDateTime('2026-01-01 00:00:00', 'UTC')" "d = toDateTime('2026-01-01 00:00:00', 'UTC')" \
    "d > ${TOKYO_MIDNIGHT}" "d > toDateTime64('2026-01-01 00:00:00', 0, 'Asia/Tokyo')" \
    "d < toDateTime('2026-01-01 12:00:00')" "d <= toDate('2026-01-01')" "d < '2026-01-01'" \
    "toDate32(d) < ${DT}" "toDate(d) > ${TOKYO_MIDNIGHT}"; do
    check dates UTC "${where}"
done
# `Date32` and `DateTime` compare in a `DateTime64` of the session time zone.
check dates Pacific/Kiritimati "d < ${DT}"
check dates Pacific/Kiritimati "d < toDateTime64('2026-01-01 12:00:00', 0, 'UTC')"

# A `Date` compared with a `DateTime` is out of order outside the `DateTime` range.
make_table epoch d Date32 date 0 '["1970-01-01"]' '["2026-01-02"]' '["2120-01-01"]'
check epoch UTC "toDate(d) > ${TOKYO_MIDNIGHT}"
check epoch UTC "toDate(d) < ${DT}"
check epoch UTC "toDate(d) <= toDateTime('2106-02-07 06:28:15', 'UTC')"

# `Pacific/Apia` skipped 2011-12-30, which therefore starts at the same instant as 2011-12-31.
APIA="toDateTime64('2011-12-31 00:00:00', 0, 'Pacific/Apia')"
make_table skipped_day d Date32 date 0 '["2011-12-30"]' '["2011-12-31"]'
for where in "d = ${APIA}" "d >= ${APIA}" "NOT (d < ${APIA})" "d < ${APIA}"; do
    check skipped_day UTC "${where}"
done

# A `Date32` day outside the `DateTime64(9)` range compares as the type's largest value.
DT64_MAX="toDateTime64('2262-04-11 23:47:16.854775807', 9, 'UTC')"
make_table dt64_bound d Date32 date 0 '["2262-04-11"]' '["2262-04-12"]'
for where in "d <= ${DT64_MAX}" "${DT64_MAX} >= d" "d < toDateTime64('2262-04-11 12:00:00', 9, 'UTC')"; do
    check dt64_bound UTC "${where}"
done

make_table dates_nullable d 'Nullable(Date32)' date 0 '["2026-01-01"]' '["2026-01-02"]'
check dates_nullable UTC "d < ${DT}"

make_table bytes x Int8 byte 0 '[50, 60]' '[1, 2]' '[44, 44]'
for where in "x < 300" "x != 300" "x > -200" "x < 1.5" "x < 45" "x = 44"; do
    check bytes UTC "${where}"
done

make_table bools b Bool boolean 0 '[true, true]' '[false, false]'
for where in "b != 5" "b != 256" "b < 2" "b > 0.5" "b = 1" "NOT (b = 1)" "b = true" \
    "b != reinterpret(toUInt8(5), 'Bool')" "b < reinterpret(toUInt8(5), 'Bool')"; do
    check bools UTC "${where}"
done

make_table strings s String string 0 '["a"]' '["b"]'
for where in "s = 'a'" "s < 'b'" "s = toFixedString('a', 3)"; do
    check strings UTC "${where}"
done

make_table cdf d Date32 date 1 '["2025-12-31"]' '["2026-01-01"]' '["2026-01-02"]'
check cdf UTC "d < ${DT}"
check cdf UTC "d > ${TOKYO_MIDNIGHT}"
check cdf Pacific/Kiritimati "d < ${DT}"

run_all
