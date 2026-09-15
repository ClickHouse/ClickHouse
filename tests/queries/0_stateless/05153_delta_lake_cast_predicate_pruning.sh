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

# A clickhouse-local launch costs seconds on slow builds, and one launch per query used to blow
# past the 180s test limit. The test therefore records everything first - `make_table`, `check`
# and `expect_error` only declare work - and `run_all` at the bottom executes it in five batched
# launches: one for the data files, then one per engine-predicate mode for the checks and one
# per mode for the expected errors. Per-query numbers are recovered from consecutive reads of
# the cumulative counters in `system.events` and `system.errors`, which is exact because the
# statements of one launch run sequentially in a single process.

TABLES=()
declare -A TABLE_TYPE TABLE_STATS TABLE_DATES
FIXTURE_QUERIES=""

# make_table <table> <column type> <full|none|no_min|no_max> <dates>...
# Each argument is one file: sorted, comma-separated ISO dates, or a single NULL.
# `no_min` / `no_max` omit only that bound; `none` emits only `numRecords`.
make_table() {
    local table=$1 type=$2 stats_mode=$3
    shift 3
    mkdir -p "${ROOT}/${table}/_delta_log"
    TABLES+=("${table}")
    TABLE_TYPE[$table]="${type}"
    TABLE_STATS[$table]="${stats_mode}"
    TABLE_DATES[$table]="$*"
    local date value
    for date in "$@"; do
        value="'${date//,/\',\'}'"
        [[ "${date}" == NULL ]] && value=NULL
        FIXTURE_QUERIES+="
            INSERT INTO FUNCTION file('${ROOT}/${table}/${date}.parquet', Parquet, 'd ${type}')
            SELECT CAST(arrayJoin([${value}]), '${type}');"
    done
}

# The second phase of `make_table`: protocol, metaData, then one add action per file. The log is
# hand-written because ClickHouse's Delta writer emits no per-column stats, and it can only be
# written after the fixture batch ran - it embeds the data files' byte sizes. `schemaString` and
# `stats` are JSON documents embedded as JSON strings, hence the escaped quotes.
write_log() {
    local table=$1
    local type="${TABLE_TYPE[$table]}" stats_mode="${TABLE_STATS[$table]}"
    local nullable=false
    [[ "${type}" == Nullable* ]] && nullable=true
    local log="${ROOT}/${table}/_delta_log/00000000000000000000.json"
    cat > "${log}" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-${table}","format":{"provider":"parquet","options":{}},"schemaString":"{\\"type\\":\\"struct\\",\\"fields\\":[{\\"name\\":\\"d\\",\\"type\\":\\"date\\",\\"nullable\\":${nullable},\\"metadata\\":{}}]}","partitionColumns":[],"configuration":{}}}
EOF
    local date stats size
    local -a all_dates dates
    read -r -a all_dates <<< "${TABLE_DATES[$table]}"
    for date in "${all_dates[@]}"; do
        IFS=, read -r -a dates <<< "${date}"
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

ITEMS=()
CHECK_TABLES=() CHECK_WHERES=() CHECK_SETTINGS=()
ERR_HEADERS=() ERR_TABLES=() ERR_WHERES=() ERR_ENABLED=() ERR_CODES=() ERR_SETTINGS=()
OFF_ROWS=() OFF_DELTAS=() ON_ROWS=() ON_DELTAS=()

# check <table> <where> [settings]: rows with the engine predicate on must match the rows with
# it off, the off run must scan every file, and the on rows and scan count go to the reference.
check() {
    ITEMS+=("check ${#CHECK_TABLES[@]}")
    CHECK_TABLES+=("$1")
    CHECK_WHERES+=("$2")
    CHECK_SETTINGS+=("${3:-}")
}

# expect_error <header> <table> <where> <enabled> <error> [settings]: the query must fail with
# the given error identity; scan counts are not pinned after cancellation.
expect_error() {
    ITEMS+=("error ${#ERR_HEADERS[@]}")
    ERR_HEADERS+=("$1")
    ERR_TABLES+=("$2")
    ERR_WHERES+=("$3")
    ERR_ENABLED+=("$4")
    ERR_CODES+=("$5")
    ERR_SETTINGS+=("${6:-}")
}

# The data query of one recorded case, with its per-case settings attached.
data_query() {
    printf "SELECT arraySort(groupArray(ifNull(toString(d), 'NULL'))) FROM deltaLakeLocal('%s') WHERE %s%s;" \
        "${ROOT}/$1" "$2" "${3:+ SETTINGS $3}"
}

# Disable every applicable Parquet pruning path, so only the engine predicate can skip files.
run_batch() {
    local enabled=$1
    shift
    ${CLICKHOUSE_LOCAL} --date_time_overflow_behavior=ignore --convert_query_to_cnf=0 \
        --short_circuit_function_evaluation=disable \
        --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 \
        --input_format_parquet_bloom_filter_push_down=0 --input_format_parquet_dictionary_filter_push_down=0 \
        --delta_lake_enable_engine_predicate="${enabled}" --delta_lake_throw_on_engine_predicate_error=1 "$@"
}

# Runs every recorded check in one launch. Each data query is followed by a read of the
# cumulative `DeltaLakeScannedFiles` counter; consecutive differences give per-query scan counts.
run_checks() {
    local enabled=$1
    local -n rows_out=$2 deltas_out=$3
    local i batch="" out
    for i in "${!CHECK_TABLES[@]}"; do
        batch+="$(data_query "${CHECK_TABLES[$i]}" "${CHECK_WHERES[$i]}" "${CHECK_SETTINGS[$i]}")
            SELECT sumIf(value, event = 'DeltaLakeScannedFiles') FROM system.events;"
    done
    out=$(run_batch "${enabled}" --query "${batch}")
    local -a lines
    mapfile -t lines <<< "${out}"
    if [[ "${#lines[@]}" != "$(( 2 * ${#CHECK_TABLES[@]} ))" ]]; then
        printf 'Expected %s lines from the check batch (engine predicate %s), got %s:\n%s\n' \
            "$(( 2 * ${#CHECK_TABLES[@]} ))" "${enabled}" "${#lines[@]}" "${out}" >&2
        return 1
    fi
    local previous=0 cumulative
    for i in "${!CHECK_TABLES[@]}"; do
        rows_out+=("${lines[2 * i]}")
        cumulative="${lines[2 * i + 1]}"
        deltas_out+=("$(( cumulative - previous ))")
        previous="${cumulative}"
    done
}

# Runs the recorded error cases of one mode in one launch under `--ignore-error`, which in
# clickhouse-local silently skips a failed query and continues. A failed SELECT contributes no
# result row, so each case must produce exactly one line: the cumulative count of its expected
# error code, read from `system.errors` right after it. The count must grow across the case,
# which both proves the query failed and pins the error identity.
run_errors() {
    local enabled=$1
    local i batch="" out
    local -a case_ids=()
    for i in "${!ERR_HEADERS[@]}"; do
        [[ "${ERR_ENABLED[$i]}" == "${enabled}" ]] || continue
        case_ids+=("$i")
        batch+="$(data_query "${ERR_TABLES[$i]}" "${ERR_WHERES[$i]}" "${ERR_SETTINGS[$i]}")
            SELECT sumIf(value, name = '${ERR_CODES[$i]}') FROM system.errors;"
    done
    out=$(run_batch "${enabled}" --ignore-error --query "${batch}") || true
    local -a lines
    mapfile -t lines <<< "${out}"
    if [[ "${#lines[@]}" != "${#case_ids[@]}" ]]; then
        printf 'Expected %s failing queries (engine predicate %s), got output:\n%s\n' \
            "${#case_ids[@]}" "${enabled}" "${out}" >&2
        return 1
    fi
    local j code
    declare -A previous=()
    for j in "${!case_ids[@]}"; do
        i="${case_ids[$j]}"
        code="${ERR_CODES[$i]}"
        if [[ ! "${lines[$j]}" =~ ^[0-9]+$ ]] || [[ "${lines[$j]}" -le "${previous[$code]:-0}" ]]; then
            printf 'Expected %s (engine predicate %s) from: %s\n' "${code}" "${enabled}" "${ERR_WHERES[$i]}" >&2
            return 1
        fi
        previous[$code]="${lines[$j]}"
    done
}

# Prints the results in declaration order; the reference interleaves checks and expected errors.
emit_results() {
    local item kind idx
    for item in "${ITEMS[@]}"; do
        read -r kind idx <<< "${item}"
        if [[ "${kind}" == error ]]; then
            printf '%s\n' "${ERR_HEADERS[$idx]}"
            printf '1\n'
            continue
        fi
        local table="${CHECK_TABLES[$idx]}" where="${CHECK_WHERES[$idx]}" settings="${CHECK_SETTINGS[$idx]}"
        local files=("${ROOT}/${table}/"*.parquet)
        printf '%s: %s%s\n' "${table}" "${where}" "${settings:+ SETTINGS ${settings}}"
        # Compare rows, not scan counts. The reference independently checks rows and selected files.
        diff -u <(printf '%s\n' "${OFF_ROWS[$idx]}") <(printf '%s\n' "${ON_ROWS[$idx]}")
        if [[ "${OFF_DELTAS[$idx]}" != "${#files[@]}" ]]; then
            printf 'Unexpected unpruned file count %s for: %s\n' "${OFF_DELTAS[$idx]}" "${where}" >&2
            return 1
        fi
        printf '%s\n' "${ON_ROWS[$idx]}"
        printf '%s\n' "${ON_DELTAS[$idx]}"
    done
}

run_all() {
    ${CLICKHOUSE_LOCAL} --query "${FIXTURE_QUERIES}"
    local table
    for table in "${TABLES[@]}"; do
        write_log "${table}"
    done
    run_checks 0 OFF_ROWS OFF_DELTAS
    run_checks 1 ON_ROWS ON_DELTAS
    run_errors 0
    run_errors 1
    emit_results
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
    expect_error "nullable: cast rejects NULL (engine predicate ${enabled})" \
        nullable "CAST(d AS Date) = '2026-01-01'" "${enabled}" \
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
            expect_error "${table}: ${where} (engine predicate ${enabled}, throw)" \
                "${table}" "${where}" "${enabled}" VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE \
                "date_time_overflow_behavior='throw'"
        done
    done
done

# The optional timezone argument of `toDate` / `toDate32` is inert for a `Date32` source.
check t "toDate(d, 'UTC') = '2026-01-01'"
check t "toDate32(d, 'UTC') = '2026-01-01'"
check t "toDate32(d, 'UTC') != '2026-01-01'"

# Ordering: identity conversions map exactly; `Date`-narrowing ones are fenced so out-of-domain files stay scanned in either polarity.
check t "toDate32(d) < '2026-01-02'"
check t "toDate32(d) >= '2026-01-02'"
check t "'2026-01-02' > toDate32(d)"
check t "d::Date32 <= '2026-01-01'"
check t "toDate(d) < '2026-01-02'"
check t "toDate(d) >= '2026-01-02'"
check t "'2026-01-02' > toDate(d)"
check t "NOT (toDate(d) < '2026-01-02')"
check t "NOT (toDate(d) > '2026-01-01')"
check aliases "toDate(d) <= '2026-01-01'"
check lower "toDate(d) < '1970-01-02'" "date_time_overflow_behavior='saturate'"
check upper "toDate(d) > '2149-06-05'" "date_time_overflow_behavior='saturate'"
# Identity ordering can still prune from a single remaining bound.
check no_min "toDate32(d) > '2026-01-01'"
check no_max "toDate32(d) < '2026-01-02'"
# An actual overflow must survive an ordering comparison whose in-domain answer is false.
for enabled in 0 1; do
    expect_error "upper: toDate(d) < '2026-01-01' (engine predicate ${enabled}, throw)" \
        upper "toDate(d) < '2026-01-01'" "${enabled}" VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE \
        "date_time_overflow_behavior='throw'"
done

# Mixed `Date` / `Date32` literals convert exactly; one beyond the `Date` domain stays untranslated and scans everything.
check t "toDate32(d) = toDate('2026-01-01')"
check t "toDate(d) = toDate32('2026-01-01')"
check t "toDate(d) != toDate32('2026-01-01')"
check t "toDate32(d) < toDate('2026-01-02')"
check t "toDate(d) = toDate32('2205-06-08')"

run_all
