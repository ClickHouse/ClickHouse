#!/usr/bin/env bash
# Tags: long, no-shared-merge-tree, no-object-storage
# Regression test: a crafted text-index posting list whose decoded row ids wrap / fall outside the
# segment row range must be rejected with CORRUPTED_DATA, not decoded into an out-of-bounds write in
# the lazy cursor's `padColumn`. Before the fix this crashed the server with a segmentation fault
# (padColumn / linearSegments in MergeTreeIndexTextPostingListCursor.cpp).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_text_oob"

# Bitpacking codec + posting_list_block_size 1048576: the common token 'zlast' (present in every row)
# then gets a first posting segment covering rows [0, 1048575] that is read through the lazy cursor.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE t_text_oob
(
    id UInt64,
    str String,
    INDEX idx str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 1048576) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS text_index_posting_list_codec = 'bitpacking', min_bytes_for_full_part_storage = 0
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_text_oob SELECT number, concat('tok', toString(number % 500), ' zlast') FROM numbers(1050000)"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE t_text_oob FINAL"

DATA_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_text_oob' AND active")
PST="${DATA_PATH}skp_idx_idx.pst.idx"

# Corrupt 'zlast''s first posting segment header. The header is
#   [codec=Bitpacking][payload_bytes][cardinality][first_row_id][payload...][index section]
# with all counts as VarUInt. Locate it by the unique byte sequence VarUInt(1048576) + VarUInt(0)
# (cardinality of the first dense segment followed by first_row_id 0), then:
#   * cardinality 1048576 -> 1048575 (same VarUInt length) so the dense-segment shortcut does not
#     fire and the block is actually decoded;
#   * first_row_id 0 -> 0xFFFFFFF0 so the uint32 delta `inclusive_scan` wraps, producing a
#     non-monotonic, out-of-range row-id array.
python3 - "$PST" <<'PY'
import sys
path = sys.argv[1]
data = bytearray(open(path, "rb").read())
needle = bytes([0x80, 0x80, 0x40, 0x00])  # VarUInt(1048576) + VarUInt(0)
i = data.find(needle)
assert i != -1 and data.find(needle, i + 1) == -1, "expected exactly one first-segment header match"
data[i:i + 3] = bytes([0xFF, 0xFF, 0x3F])                    # cardinality -> 1048575
data[i + 3:i + 4] = bytes([0xF0, 0xFF, 0xFF, 0xFF, 0x0F])    # first_row_id -> 0xFFFFFFF0
open(path, "wb").write(data)
PY

# force_data_skipping_indices routes the query through the lazy posting-list cursor; disabling the
# count-from-text-index optimization keeps it on the row-id apply path that dereferences the decoded ids.
${CLICKHOUSE_CLIENT} --query "
SET query_plan_optimize_count_from_text_index = 0, force_data_skipping_indices = 'idx';
SELECT count(id) FROM t_text_oob WHERE hasToken(str, 'zlast')
" 2>&1 | grep -oF "CORRUPTED_DATA" | head -1

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_text_oob"
