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

# make_table <table> <column type> <full|none> <date>...
#   <table>        directory under ROOT, also the label `check` prints
#   <column type>  Date32 or Nullable(Date32)
#   <full|none>    per-file stats: real min/max, or `numRecords` only
#   <date>...      one single-row Parquet file each; the literal NULL makes an all-null file
#
# The v0 transaction log is authored by hand because ClickHouse's own Delta writer emits no
# per-column stats, and a statsless table cannot prune. Singleton files make the pruning
# expectations map 1:1 to dates.
make_table() {
    local table=$1 type=$2 stats_mode=$3
    shift 3

    ## Phase 1: the data files. Written first - the log below embeds their byte sizes.
    mkdir -p "${ROOT}/${table}/_delta_log"
    local date value
    for date in "$@"; do
        value="'${date}'"
        [[ "${date}" == NULL ]] && value=NULL
        ${CLICKHOUSE_LOCAL} --query "
            INSERT INTO FUNCTION file('${ROOT}/${table}/${date}.parquet', Parquet, 'd ${type}')
            SELECT CAST(${value}, '${type}')"
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
    for date in "$@"; do
        if [[ "${stats_mode}" == none ]]; then
            stats="{\"numRecords\":1}"
        elif [[ "${date}" == NULL ]]; then
            stats="{\"numRecords\":1,\"nullCount\":{\"d\":1}}"
        else
            stats="{\"numRecords\":1,\"minValues\":{\"d\":\"${date}\"},\"maxValues\":{\"d\":\"${date}\"},\"nullCount\":{\"d\":0}}"
        fi
        size=$(wc -c < "${ROOT}/${table}/${date}.parquet")
        cat >> "${log}" <<EOF
{"add":{"path":"${date}.parquet","partitionValues":{},"size":${size},"modificationTime":1700000000000,"dataChange":true,"stats":"${stats//\"/\\\"}"}}
EOF
    done
}

# Each fresh process prints original dates, then the process-global selected-file counter.
check() {
    local table="$1" where="$2"
    printf '%s: %s%s\n' "$table" "$where" "${3:+ SETTINGS $3}"
    ${CLICKHOUSE_LOCAL} --date_time_overflow_behavior=ignore --convert_query_to_cnf=0 \
        --delta_lake_throw_on_engine_predicate_error=1 --query "
        SELECT arraySort(groupArray(d)) FROM deltaLakeLocal('${ROOT}/${table}') WHERE ${where} ${3:+SETTINGS $3};
        SELECT sumIf(value, event = 'DeltaLakeScannedFiles') FROM system.events;
    "
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
check t "CAST(d AS Date) = '2026-01-01'" "delta_lake_enable_engine_predicate=0"
check t "d::Date != '2026-01-01'" "delta_lake_enable_engine_predicate=0"

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
check nullable "toDate(d) = '2026-01-01'" "delta_lake_enable_engine_predicate=0"

make_table no_stats Date32 none 2026-01-01 2026-01-02
check no_stats "toDate(d) = '2026-01-01'"

# A non-nullable cast must still reject the stats-bearing NULL file.
for enabled in 0 1; do
    printf 'nullable: cast rejects NULL (engine predicate %s)\n' "$enabled"
    ${CLICKHOUSE_LOCAL} --delta_lake_enable_engine_predicate="$enabled" --cast_keep_nullable=0 \
        --input_format_parquet_filter_push_down=0 --delta_lake_throw_on_engine_predicate_error=1 \
        --query "SELECT count() FROM deltaLakeLocal('${ROOT}/nullable') WHERE CAST(d AS Date) = '2026-01-01'" \
        2>&1 | grep -c CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN
done
