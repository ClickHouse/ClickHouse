#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

ROOT="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_delta_cast"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

# make_table <table> <column type> <full|none|no_min|no_max> <dates>...
# Each argument is one file: sorted, comma-separated ISO dates, or a single NULL.
# `no_min` / `no_max` omit only that bound; `none` emits only `numRecords`.
# The log is hand-written because ClickHouse's Delta writer emits no per-column stats.
make_table() {
    local table=$1 type=$2 stats_mode=$3
    shift 3

    ## Phase 1: the data files. Written first - the log below embeds their byte sizes.
    mkdir -p "${ROOT}/${table}/_delta_log"
    local date value
    for date in "$@"; do
        value="'${date//,/\',\'}'"
        [[ "${date}" == NULL ]] && value=NULL
        ${CLICKHOUSE_LOCAL} --query "
            INSERT INTO FUNCTION file('${ROOT}/${table}/${date}.parquet', Parquet, 'd ${type}')
            SELECT CAST(arrayJoin([${value}]), '${type}')"
    done

    ## Phase 2: the log - protocol, metaData, then one add action per file. schemaString and
    ## stats are JSON documents embedded as JSON strings, hence the escaped quotes.
    local nullable=false
    [[ "${type}" == Nullable* ]] && nullable=true
    local log="${ROOT}/${table}/_delta_log/00000000000000000000.json"
    cat > "${log}" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-${table}","format":{"provider":"parquet","options":{}},"schemaString":"{\\"type\\":\\"struct\\",\\"fields\\":[{\\"name\\":\\"d\\",\\"type\\":\\"date\\",\\"nullable\\":${nullable},\\"metadata\\":{}}]}","partitionColumns":[],"configuration":{}}}
EOF
    local stats size
    local -a dates
    for date in "$@"; do
        IFS=, read -r -a dates <<< "$date"
        stats="{\"numRecords\":${#dates[@]}"
        if [[ "${stats_mode}" != none ]]; then
            if [[ "${date}" == NULL ]]; then
                stats+=",\"nullCount\":{\"d\":1}"
            else
                [[ "${stats_mode}" == no_min ]] || stats+=",\"minValues\":{\"d\":\"${date%%,*}\"}"
                [[ "${stats_mode}" == no_max ]] || stats+=",\"maxValues\":{\"d\":\"${date##*,}\"}"
                stats+=",\"nullCount\":{\"d\":0}"
            fi
        fi
        stats+="}"
        size=$(wc -c < "${ROOT}/${table}/${date}.parquet")
        cat >> "${log}" <<EOF
{"add":{"path":"${date}.parquet","partitionValues":{},"size":${size},"modificationTime":1700000000000,"dataChange":true,"stats":"${stats//\"/\\\"}"}}
EOF
    done
}

# Fresh processes isolate `system.events`. Disable every applicable Parquet pruning path.
query() {
    local table="$1" where="$2" enabled="$3"
    ${CLICKHOUSE_LOCAL} --date_time_overflow_behavior=ignore --convert_query_to_cnf=0 \
        --short_circuit_function_evaluation=disable \
        --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 \
        --input_format_parquet_bloom_filter_push_down=0 --input_format_parquet_dictionary_filter_push_down=0 \
        --delta_lake_enable_engine_predicate="$enabled" --delta_lake_throw_on_engine_predicate_error=1 --query "
        SELECT arraySort(groupArray(ifNull(toString(d), 'NULL')))
        FROM deltaLakeLocal('${ROOT}/${table}') WHERE ${where} ${4:+SETTINGS $4};
        SELECT sumIf(value, event = 'DeltaLakeScannedFiles') FROM system.events;
    "
}

# Compare rows, not scan counts. The reference independently checks rows and selected files.
check() {
    local table="$1" where="$2" off on
    local files=("${ROOT}/${table}/"*.parquet)
    printf '%s: %s%s\n' "$table" "$where" "${3:+ SETTINGS $3}"
    off=$(query "$table" "$where" 0 "${3:-}")
    on=$(query "$table" "$where" 1 "${3:-}")
    diff -u <(printf '%s\n' "${off%$'\n'*}") <(printf '%s\n' "${on%$'\n'*}")
    if [[ "${off##*$'\n'}" != "${#files[@]}" ]]; then
        printf 'Unexpected unpruned file count: %s\n' "$off" >&2
        return 1
    fi
    printf '%s\n' "$on"
}

# Check both failure status and error identity; do not pin scan counts after cancellation.
expect_error() {
    local table="$1" where="$2" enabled="$3" error="$4" output
    if output=$(query "$table" "$where" "$enabled" "${5:-}" 2>&1); then
        printf 'Expected %s, query succeeded: %s\n' "$error" "$where" >&2
        return 1
    fi
    if [[ "$output" != *"($error)"* ]]; then
        printf 'Expected %s, got: %s\n' "$error" "$output" >&2
        return 1
    fi
    printf '1\n'
}

# `Date` wraps modulo 65536 days: the alias matches equality but stays scanned even for `!=`.
make_table t Date32 full 2026-01-01 2026-01-02 2205-06-08
check t "d = '2026-01-01'"
check t "CAST(d AS Date) = '2026-01-01'"
check t "d::Date = '2026-01-01'"
check t "toDate(d) = '2026-01-01'"
check t "d::Date32 = '2026-01-01'"
check t "toDate32(d) = toDate32('2026-01-01')"
check t "d::Date != '2026-01-01'"
check t "toDate('2026-01-01') = toDate(d)"
check t "toDate('2026-01-01') != toDate(d)"
check t "toDate(d) = '2026-01-03'"
check t "toDate32(d) = '2026-01-03'"

make_table lower Date32 full 1969-12-31 1970-01-01 1970-01-02
check lower "toDate(d) = '1970-01-01'" "date_time_overflow_behavior='saturate'"
check lower "toDate(d) != '1970-01-01'" "date_time_overflow_behavior='saturate'"
make_table upper Date32 full 2149-06-05 2149-06-06 2149-06-07
check upper "CAST(d AS Date) = '2149-06-06'" "date_time_overflow_behavior='saturate'"
check upper "CAST(d AS Date) != '2149-06-06'" "date_time_overflow_behavior='saturate'"

make_table negation Date32 full 2026-01-01 2026-01-02 2205-06-09
check negation "NOT (toDate(d) = '2026-01-01' AND d > toDate32('1970-01-01'))"

# Aliases at -65536 and +131072 days also lie outside the DateLUT's 1900..2299 fast range.
make_table aliases Date32 full 1846-07-28 2026-01-02 2384-11-12
check aliases "toDate(d) = '2026-01-01'"
check aliases "toDate(d) != '2026-01-01'"

make_table valid Date32 full 1970-01-02 2026-01-01
check valid "toDate(d) != toDateTime('2026-01-01 00:00:01', 'UTC')"
check valid "CAST(d AS Date) = '2026-01-01'" "date_time_overflow_behavior='throw'"
check valid "toDate(d) != '2026-01-01'" "date_time_overflow_behavior='throw'"

make_table nullable 'Nullable(Date32)' full NULL 2026-01-01 2026-01-02 2205-06-08
check nullable "toDate(d) = '2026-01-01'"
check nullable "CAST(d AS Nullable(Date)) = toNullable(toDate('2026-01-01'))"
check nullable "CAST(d AS Nullable(Date)) != '2026-01-01'"

make_table no_stats Date32 none 2026-01-01 2026-01-02
check no_stats "toDate(d) = '2026-01-01'"

# A non-nullable cast must still reject the stats-bearing NULL file.
for enabled in 0 1; do
    printf 'nullable: cast rejects NULL (engine predicate %s)\n' "$enabled"
    expect_error nullable "CAST(d AS Date) = '2026-01-01'" "$enabled" \
        CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN "cast_keep_nullable=0"
done

# `groupArray` must not hide the all-null file in the unfiltered fixture check.
check nullable "1"

# Two files cross the Date boundaries; two safe singletons must still be prunable.
make_table mixed Date32 full 1969-12-31,1970-01-01 2149-06-06,2149-06-07 2026-01-01 2026-01-02
for mode in ignore saturate; do
    for boundary in 1970-01-01 2149-06-06; do
        check mixed "toDate(d) = '${boundary}'" "date_time_overflow_behavior='${mode}'"
        check mixed "toDate(d) != '${boundary}'" "date_time_overflow_behavior='${mode}'"
    done
done

# Both endpoints wrap to day zero, but the file also contains a matching interior value.
make_table spanning Date32 full 1970-01-01,1970-01-02,2149-06-07 2026-01-01
check spanning "toDate(d) = '1970-01-02'"
check spanning "NOT (toDate(d) != '1970-01-02')"

# One missing bound cannot prove that a Date cast is safe.
for mode in no_min no_max; do
    make_table "$mode" Date32 "$mode" 2026-01-01 2026-01-02
    check "$mode" "toDate(d) = '2026-01-01'"
    check "$mode" "toDate(d) != '2026-01-01'"
done
# Identity conversions can still prune from the remaining bound.
check no_min "toDate32(d) = '2026-01-02'"
check no_max "toDate32(d) = '2026-01-01'"

# Actual overflows at both bounds must survive equality, inequality, and nested negation.
for table in lower upper; do
    for where in "CAST(d AS Date) = '2026-01-01'" "toDate(d) != '2026-01-01'" \
        "NOT (toDate(d) = '2026-01-01' AND d > toDate32('1900-01-01'))"; do
        for enabled in 0 1; do
            printf '%s: %s (engine predicate %s, throw)\n' "$table" "$where" "$enabled"
            expect_error "$table" "$where" "$enabled" VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE \
                "date_time_overflow_behavior='throw'"
        done
    done
done
