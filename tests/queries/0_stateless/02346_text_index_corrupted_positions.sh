#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree, no-object-storage, no-random-merge-tree-settings
#
# A damaged positions stream (.pos) must raise an error, never answer hasPhrase from the
# garbage it decodes. The blocked positions layout is self-describing -- document count, block
# count and per-block payload sizes -- so the reader validates every declared size against the
# token's own blob before using it. Without those checks a corrupt directory decodes cleanly
# often enough to return wrong matches, which is far worse than failing the query.
#
# .pos is written uncompressed (plain_hashing), so the bytes edited here reach the decoder
# directly instead of tripping a decompression checksum first.
#
# no-fasttest / no-object-storage / no-shared / no-replicated: local on-disk part-file surgery.
# no-random-merge-tree-settings: needs standalone (non-packed) index files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
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
-- The phrase is selective (100 of 2000 rows) so the reader takes the positional path rather
-- than the max-selectivity fallback, which would evaluate on column data and never read .pos.
INSERT INTO t_pos SELECT number, if(number < 100, 'needle alpha beta', concat('hello', number % 50, ' world', number % 50)) FROM numbers(2000);
"

PART=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_pos' AND active LIMIT 1")
POS="${PART}skp_idx_txt.pos.idx"

# Runs hasPhrase through the index and reports 'error' or the row count, so a corrupt part
# that silently answers is distinguishable from one that fails.
phrase_via_index () {
    ${CLICKHOUSE_CLIENT} -q "
    SYSTEM DROP TEXT INDEX CACHES;
    SYSTEM DROP MARK CACHE;
    SYSTEM DROP UNCOMPRESSED CACHE;" >/dev/null 2>&1
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

# Control: the intact index must agree with a plain scan, otherwise the corruption cases below
# would prove nothing.
echo "intact_matches_scan:"
${CLICKHOUSE_CLIENT} -q "
SELECT (SELECT count() FROM t_pos WHERE hasPhrase(s, 'needle alpha') SETTINGS use_skip_indexes = 0)
     = (SELECT count() FROM t_pos WHERE hasPhrase(s, 'needle alpha')
        SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1)"

cp "${POS}" "${POS}.orig"

# Zeroed directory: the stored document count no longer matches the dictionary's, and a
# zero-length block cannot hold a document.
: > "${POS}"
head -c "$(stat -c%s "${POS}.orig")" /dev/zero > "${POS}"
echo "zeroed_directory:"
phrase_via_index

# Truncated blob: the directory's declared sizes now run past the end of the token's data.
cp "${POS}.orig" "${POS}"
truncate -s 3 "${POS}"
echo "truncated_blob:"
phrase_via_index

# Oversized declared sizes: flipping the high bits of the directory's leading bytes inflates
# the counts and payload lengths it declares.
cp "${POS}.orig" "${POS}"
printf '\xff\xff\xff\xff' | dd of="${POS}" bs=1 seek=0 conv=notrunc status=none
echo "inflated_directory:"
phrase_via_index

# A block size that runs past this token's blob but stays inside the file. The first token in
# .pos is 'alpha' (100 rows), whose directory is (num_docs=100, num_blocks=1, block_bytes=3)
# as single-byte varints, so byte 2 is the block size and the whole blob is 6 bytes. Declaring
# 100 bytes there still fits the file, so only a bound taken from the token's own length rejects
# it -- bounding by the rest of the file would decode the following tokens' bytes as this one's.
cp "${POS}.orig" "${POS}"
echo "fixture_directory_bytes:"
od -An -tu1 -N 3 "${POS}" | tr -s ' '
printf '\x64' | dd of="${POS}" bs=1 seek=2 conv=notrunc status=none
echo "declared_size_past_blob:"
phrase_via_index

# Restored: the same query works again, so the failures above came from the bytes and not
# from a table left permanently unusable.
cp "${POS}.orig" "${POS}"
echo "restored_matches_scan:"
${CLICKHOUSE_CLIENT} -q "
SYSTEM DROP TEXT INDEX CACHES;
SELECT (SELECT count() FROM t_pos WHERE hasPhrase(s, 'needle alpha') SETTINGS use_skip_indexes = 0)
     = (SELECT count() FROM t_pos WHERE hasPhrase(s, 'needle alpha')
        SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1)"

rm -f "${POS}.orig"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_pos SYNC"
