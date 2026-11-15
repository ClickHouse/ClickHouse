#!/usr/bin/env bash

# A broken projection does not break its part, on purpose. But the part's own consistency check used to
# be skipped whenever one of its projections was broken, which took the part's crash-corruption
# protection away with it: a marks file left empty by a power loss loads as an empty `index_granularity`,
# the row count becomes zero, and the acknowledged rows disappear from every query with nothing detached
# to recover them from. The part is detached as broken now, while a broken projection alone still is not.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP}/05214_broken_projection_does_not_skip_part_check"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

COMPACT="min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000"
WIDE="min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0"
# The marks are small, so that each marks file consists of several compressed blocks.
SMALL_MARKS="index_granularity = 100, marks_compress_block_size = 256"
# The data is in small compressed blocks, so that each data file consists of several of them.
SMALL_BLOCKS="min_compress_block_size = 1024, max_compress_block_size = 1024"
# Non-adaptive granularity has no final mark, so the marks tell only where the last granule starts.
NON_ADAPTIVE="index_granularity_bytes = 0"

# Every case is a table of its own, so that all of them are created by one run of `clickhouse-local`
# and loaded by another one.
TABLES="
    both
    projection_only
    both_no_checksums
    projection_only_no_checksums
    wide_no_checksums
    wide_torn_first_column_no_checksums
    wide_torn_second_column_no_checksums
    wide_missing_second_column_no_checksums
    compact_torn_no_checksums
    wide_several_blocks_no_checksums
    compact_several_blocks_no_checksums
    wide_dynamic_no_substreams_no_checksums
    wide_torn_dynamic_no_substreams_no_checksums
    wide_missing_dynamic_no_substreams_no_checksums
    wide_empty_checksums
    wide_torn_first_column_empty_checksums
    wide_missing_second_column_empty_checksums
    compact_torn_empty_checksums
    wide_missing_dynamic_no_substreams_empty_checksums
    wide_missing_data_empty_checksums
    compact_missing_data_empty_checksums
    wide_missing_dynamic_data_no_substreams_no_checksums
    wide_missing_dynamic_data_no_substreams_empty_checksums
    wide_torn_data_no_checksums
    compact_torn_data_no_checksums
    wide_torn_data_empty_checksums
    wide_non_adaptive_no_checksums
    wide_non_adaptive_torn_tail_no_checksums
"

table_settings()
{
    case "$1" in
        wide_no_checksums|wide_empty_checksums) echo "${WIDE}" ;;
        wide_non_adaptive_*) echo "${WIDE}, ${SMALL_MARKS}, ${SMALL_BLOCKS}, ${NON_ADAPTIVE}" ;;
        wide_several_blocks_*|wide_torn_data_*) echo "${WIDE}, ${SMALL_MARKS}, ${SMALL_BLOCKS}" ;;
        compact_several_blocks_*|compact_torn_data_*) echo "${COMPACT}, ${SMALL_MARKS}, ${SMALL_BLOCKS}" ;;
        wide_*) echo "${WIDE}, ${SMALL_MARKS}" ;;
        compact_*) echo "${COMPACT}, ${SMALL_MARKS}" ;;
        *) echo "${COMPACT}" ;;
    esac
}

# A `Dynamic` column has streams that cannot be enumerated without the deserialization state.
table_columns()
{
    case "$1" in
        *_dynamic_*) echo "id UInt64, v UInt64, d Dynamic" ;;
        *) echo "id UInt64, v UInt64" ;;
    esac
}

table_values()
{
    case "$1" in
        *_dynamic_*) echo "number, number % 10, number" ;;
        *) echo "number, number % 10" ;;
    esac
}

create_queries=""
for table in ${TABLES}
do
    create_queries+="
        CREATE TABLE ${table} ($(table_columns "${table}"), PROJECTION p (SELECT v, count() GROUP BY v))
        ENGINE = MergeTree ORDER BY id
        SETTINGS $(table_settings "${table}"), compress_marks = 1,
            ratio_of_defaults_for_sparse_serialization = 1, replace_long_file_name_to_hash = 0;
        INSERT INTO ${table} SELECT $(table_values "${table}") FROM numbers(5000);"
done

${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --multiquery -q "
    ${create_queries}
    SELECT table, part_type, rows FROM system.parts WHERE database = currentDatabase() ORDER BY table;
" </dev/null

PART_PATHS="${WORKING_DIR}.paths"
${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" -q "
    SELECT table, path FROM system.parts WHERE database = currentDatabase()" </dev/null > "${PART_PATHS}"

# A case-insensitive disk (macOS) stores every stream under the hash of its name.
COLUMN_FILES="${WORKING_DIR}.files"
${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" -q "
    SELECT table, substream, filename FROM system.parts_columns ARRAY JOIN substreams AS substream, filenames AS filename
    WHERE database = currentDatabase()" </dev/null > "${COLUMN_FILES}"

part_path()
{
    awk -F '\t' -v table="$1" '$1 == table { print $2 }' "${PART_PATHS}"
}

column_file()
{
    awk -F '\t' -v table="$1" -v substream="$2" '$1 == table && $2 == substream { print $3 }' "${COLUMN_FILES}"
}

# A compressed file cut by a crash right after its first compressed block still decompresses, only to
# fewer marks or rows.
truncate_to_first_block()
{
    local file
    for file in "$@"
    do
        if [ ! -f "${file}" ]
        then
            echo "No file ${file}, the part has: $(ls "$(dirname "${file}")")" >&2
            continue
        fi
        local compressed_size
        compressed_size=$(od -An -t u4 -j 17 -N 4 "${file}" | tr -d ' ')
        truncate -s $((16 + compressed_size)) "${file}"
    done
}

# A compressed file cut by a crash right before its last compressed block.
truncate_last_block()
{
    local file="$1"
    local file_size
    file_size=$(wc -c < "${file}" | tr -d " ")
    local offset=0
    while clickhouse_test_time_left
    do
        local compressed_size
        compressed_size=$(od -An -t u4 -j $((offset + 17)) -N 4 "${file}" | tr -d ' ')
        local next_offset=$((offset + 16 + compressed_size))
        if [ "${next_offset}" -ge "${file_size}" ]
        then
            truncate -s "${offset}" "${file}"
            return
        fi
        offset=${next_offset}
    done
}

# The marks of the part itself are gone as well as the projection's, which is what a single power loss
# can leave behind.
truncate -s 0 "$(part_path both)/data.cmrk4" "$(part_path both)/p.proj/data.cmrk4"

# Only the projection's marks are gone: the part keeps all of its rows and stays attached.
truncate -s 0 "$(part_path projection_only)/p.proj/data.cmrk4"

# The same, but `checksums.txt` of the part is gone as well, so it is regenerated from the files on disk
# while loading, and those checksums bless the empty marks file. The shape of the marks files is checked
# directly in this case.
truncate -s 0 "$(part_path both_no_checksums)/data.cmrk4" "$(part_path both_no_checksums)/p.proj/data.cmrk4"
truncate -s 0 "$(part_path projection_only_no_checksums)/p.proj/data.cmrk4"

# A compressed marks file cut by a crash right after one of its compressed blocks, and the regenerated
# checksums bless it too. First the marks of the column that the part's granularity is loaded from:
# they cover fewer rows than the part has. Then the marks of another column: they have fewer marks than
# the part's granularity.
truncate_to_first_block "$(part_path wide_torn_first_column_no_checksums)/$(column_file wide_torn_first_column_no_checksums id).cmrk2"
truncate_to_first_block "$(part_path wide_torn_second_column_no_checksums)/$(column_file wide_torn_second_column_no_checksums v).cmrk2"
truncate_to_first_block "$(part_path compact_torn_no_checksums)/data.cmrk4"

# The marks file of another column is gone altogether, so the regenerated checksums do not list it.
# `columns_substreams.txt` still names its substream.
rm "$(part_path wide_missing_second_column_no_checksums)/$(column_file wide_missing_second_column_no_checksums v).cmrk2"

# Without `columns_substreams.txt` the dynamic streams are not enumerated, but the marks of every stream
# with a data file are checked when the checksums are regenerated.
truncate_to_first_block "$(part_path wide_torn_dynamic_no_substreams_no_checksums)/$(column_file wide_torn_dynamic_no_substreams_no_checksums d.UInt64).cmrk2"
# A marks file of a dynamic stream that is gone altogether is not listed by the regenerated checksums,
# but its data file is.
rm "$(part_path wide_missing_dynamic_no_substreams_no_checksums)/$(column_file wide_missing_dynamic_no_substreams_no_checksums d.UInt64).cmrk2"

# `checksums.txt` of the oldest format lists no files at all, so it cannot vouch for the marks either.
truncate_to_first_block "$(part_path wide_torn_first_column_empty_checksums)/$(column_file wide_torn_first_column_empty_checksums id).cmrk2"
rm "$(part_path wide_missing_second_column_empty_checksums)/$(column_file wide_missing_second_column_empty_checksums v).cmrk2"
truncate_to_first_block "$(part_path compact_torn_empty_checksums)/data.cmrk4"
rm "$(part_path wide_missing_dynamic_no_substreams_empty_checksums)/$(column_file wide_missing_dynamic_no_substreams_empty_checksums d.UInt64).cmrk2"
# The data file of a stream is gone altogether while its marks are intact.
rm "$(part_path wide_missing_data_empty_checksums)/$(column_file wide_missing_data_empty_checksums v).bin"
rm "$(part_path compact_missing_data_empty_checksums)/data.bin"
# The data file of a dynamic stream is gone altogether while its marks are intact. Without
# `columns_substreams.txt` the dynamic streams are not enumerated, so the marks files found in the part
# directory are checked for their data files.
rm "$(part_path wide_missing_dynamic_data_no_substreams_no_checksums)/$(column_file wide_missing_dynamic_data_no_substreams_no_checksums d.UInt64).bin"
rm "$(part_path wide_missing_dynamic_data_no_substreams_empty_checksums)/$(column_file wide_missing_dynamic_data_no_substreams_empty_checksums d.UInt64).bin"
# A data file cut by a crash right after its first compressed block, while the marks are intact: the
# marks of the rest of the rows point past its end.
truncate_to_first_block "$(part_path wide_torn_data_no_checksums)/$(column_file wide_torn_data_no_checksums v).bin"
truncate_to_first_block "$(part_path compact_torn_data_no_checksums)/data.bin"
truncate_to_first_block "$(part_path wide_torn_data_empty_checksums)/$(column_file wide_torn_data_empty_checksums v).bin"
# Without a final mark, the marks of a data file cut inside its last granule do not point past its end:
# the last granule starts in an earlier compressed block.
truncate_last_block "$(part_path wide_non_adaptive_torn_tail_no_checksums)/$(column_file wide_non_adaptive_torn_tail_no_checksums v).bin"
for table in ${TABLES}
do
    if [[ ${table} == *_no_substreams_* ]]
    then
        rm "$(part_path "${table}")/columns_substreams.txt"
    fi
done

# Intact compressed marks of a regenerated part, also of several compressed blocks, are not mistaken
# for broken ones.
for table in ${TABLES}
do
    if [[ ${table} == *_no_checksums ]]
    then
        rm "$(part_path "${table}")/checksums.txt"
    elif [[ ${table} == *_empty_checksums ]]
    then
        echo "checksums format version: 1" > "$(part_path "${table}")/checksums.txt"
    fi
done

report_queries=""
for table in ${TABLES}
do
    report_queries+="
        SELECT '${table} rows', count() FROM ${table};
        SELECT '${table} detached parts', count(), any(reason) FROM system.detached_parts
        WHERE database = currentDatabase() AND table = '${table}';
        SELECT '${table} broken projections', countIf(is_broken) FROM system.projection_parts
        WHERE database = currentDatabase() AND table = '${table}';"
done

${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --multiquery -q "${report_queries}" </dev/null

rm -rf "${WORKING_DIR}" "${PART_PATHS}" "${COLUMN_FILES}"
