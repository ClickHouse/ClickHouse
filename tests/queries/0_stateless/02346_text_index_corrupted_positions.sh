#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database, no-shared-merge-tree, no-object-storage, no-random-merge-tree-settings
#
# A damaged positions stream (.pos) must raise an error, not answer hasPhrase from the garbage it decodes.
# .pos is uncompressed, so the bytes edited here reach the decoder instead of a decompression checksum.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
SET enable_full_text_index = 1;

DROP TABLE IF EXISTS t_pos SYNC;
CREATE TABLE t_pos
(
    k UInt64,
    s String,
    INDEX txt(s) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         index_granularity = 100, replace_long_file_name_to_hash = 0,
         packed_skip_index_max_bytes = 0,
         allow_experimental_text_index_phrase_search = 1;
-- Selective (100 of 2000 rows) so the reader takes the positional path, not the selectivity fallback.
INSERT INTO t_pos SELECT number, if(number < 100, 'needle alpha beta', concat('hello', number % 50, ' world', number % 50)) FROM numbers(2000);
"

PART=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_pos' AND active LIMIT 1")
POS="${PART}skp_idx_txt.pos.idx"

# Reports 'error' or the row count, so a part that silently answers is distinguishable from one that fails.
phrase_via_index () {
    ${CLICKHOUSE_CLIENT} -q "
    SYSTEM DROP TEXT INDEX CACHES;
    SYSTEM DROP MARK CACHE;
    SYSTEM DROP UNCOMPRESSED CACHE;
    SYSTEM DROP MMAP CACHE;
    SYSTEM DROP PAGE CACHE;" >/dev/null 2>&1
    local out
    if out=$(${CLICKHOUSE_CLIENT} -q "
        SELECT count() FROM t_pos WHERE hasPhrase(s, 'needle alpha')
        SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1,
                 query_plan_direct_read_from_text_index = 1, use_query_condition_cache = 0" 2>/dev/null)
    then
        echo "${out}"
    else
        echo "error"
    fi
}

echo "pos_stream_exists:"
if [ -s "${POS}" ]; then echo 1; else echo 0; fi

# Control: the intact index agrees with a plain scan, else the cases below would prove nothing.
echo "intact_matches_scan:"
${CLICKHOUSE_CLIENT} -q "
SELECT (SELECT count() FROM t_pos WHERE hasPhrase(s, 'needle alpha') SETTINGS use_skip_indexes = 0)
     = (SELECT count() FROM t_pos WHERE hasPhrase(s, 'needle alpha')
        SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1)"

cp "${POS}" "${POS}.orig"

# Zeroed directory: the stored document count no longer matches the dictionary's.
: > "${POS}"
head -c "$(stat -c%s "${POS}.orig")" /dev/zero > "${POS}"
echo "zeroed_directory:"
phrase_via_index

# Oversized declared sizes: high bits set in the directory's leading bytes inflate every count.
cp "${POS}.orig" "${POS}"
printf '\xff\xff\xff\xff' | dd of="${POS}" bs=1 seek=0 conv=notrunc status=none
echo "inflated_directory:"
phrase_via_index

# A block size past this token's 6-byte blob but inside the file: only a bound from the token's own
# length rejects it. Byte 2 is the first token's block size, asserted below so the fixture self-checks.
cp "${POS}.orig" "${POS}"
echo "fixture_directory_bytes:"
od -An -tu1 -N 3 "${POS}" | tr -s ' '
printf '\x64' | dd of="${POS}" bs=1 seek=2 conv=notrunc status=none
echo "declared_size_past_blob:"
phrase_via_index

# Restored: the query works again, so the failures came from the bytes, not a permanently broken table.
cp "${POS}.orig" "${POS}"
echo "restored_matches_scan:"
${CLICKHOUSE_CLIENT} -q "
SYSTEM DROP TEXT INDEX CACHES;
SYSTEM DROP MARK CACHE;
SYSTEM DROP UNCOMPRESSED CACHE;
SYSTEM DROP MMAP CACHE;
SYSTEM DROP PAGE CACHE;
SELECT (SELECT count() FROM t_pos WHERE hasPhrase(s, 'needle alpha') SETTINGS use_skip_indexes = 0)
     = (SELECT count() FROM t_pos WHERE hasPhrase(s, 'needle alpha')
        SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1)"

# Truncated blob, last: shrinking the file leaves the part's cached size stale, so any case after
# this one would fail on the stale size rather than on the bytes it means to test.
cp "${POS}.orig" "${POS}"
truncate -s 3 "${POS}"
echo "truncated_blob:"
phrase_via_index

rm -f "${POS}.orig"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_pos SYNC"
