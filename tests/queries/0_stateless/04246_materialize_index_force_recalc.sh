#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree, no-object-storage
#
# `no-object-storage`: the captured 25.8 part is a local-disk fixture with
# real file contents (`columns.txt`, `skp_idx_*.idx`, ...). On the s3 / azure
# disk those files in the table's data dir are not actual data — they are
# `DiskObjectStorageMetadata` pointer files (`<version>\n<num_objects>\n<size> <key>`)
# referring to objects in the bucket. Extracting the captured part directly
# into the local detached dir therefore fails at ATTACH time with
# `CANNOT_PARSE_INPUT_ASSERTION_FAILED` inside
# `DiskObjectStorageMetadata::deserialize`. The bug under test is a column-
# extraction issue in `splitAndModifyMutationCommands` that is independent of
# the disk layer, so exercising it on local disk is sufficient.
#
# Regression test for ClickHouse/ClickHouse#104872.
#
# Background: PR #91980 widened the force-recalculate predicate in
# `MutateFromLogEntryTask::prepare` from `!is_full_part_storage` to
# `!is_full_wide_part`, which made Compact parts force-recalculate every
# pre-existing skip index on every mutation. The follow-up
# `splitAndModifyMutationCommands` only added columns of the
# *explicitly-materialized* index to the read set. As a result a pre-existing
# index over a column that is in the table metadata but absent from the part
# on disk crashed the next mutation on that part with
# `NOT_FOUND_COLUMN_IN_BLOCK`.
#
# The broken on-disk shape was produced by 25.8: in 25.8 `MATERIALIZE INDEX`
# wrote `skp_idx_<NAME>.idx` for the new index but did *not* add the indexed
# column to the new part's `columns.txt`. After an upgrade to 25.10+ /
# 26.3+ / master, any subsequent mutation on the part hit the bug.
#
# We cannot reproduce the bug by creating a fresh table on a modern build —
# the modern `MATERIALIZE INDEX` correctly writes the index's columns to the
# new part, so the broken shape never appears. Therefore this test attaches a
# pre-captured 25.8-era Compact part (`data_104872/part_25.8.tar.gz`, 1.2 KiB)
# that has the broken shape on disk:
#
#   columns.txt: 2 columns: `timestamp`, `requestID`   (no `vid`)
#   skp_idx_vid_ix.idx, skp_idx_vid_ix.cmrk4           (present)
#
# After attaching, we replay the user's reproducer (`ADD INDEX requestID_ix`
# + `MATERIALIZE INDEX requestID_ix`). On master without the fix this
# mutation force-recalculates `vid_ix` while the read set is missing `vid`
# and we get `NOT_FOUND_COLUMN_IN_BLOCK`. With the fix the read set covers
# every pre-existing index's columns and the mutation succeeds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PART_NAME="all_1_1_0_2"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS issue_104872 SYNC"

# The schema matches the table state after `ADD COLUMN vid` + `ADD INDEX vid_ix`
# in the user's 25.8 session. The captured part was created in that exact
# session — its on-disk `columns.txt` is from before `ADD COLUMN vid` because
# 25.8's `MATERIALIZE INDEX vid_ix` did not yet add the column to the new
# part. That is the broken shape this test exercises.
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE issue_104872
(
    timestamp DateTime,
    requestID String,
    vid Int64,
    INDEX vid_ix vid TYPE bloom_filter GRANULARITY 100
)
ENGINE = MergeTree() ORDER BY timestamp
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = '10G'"

# Locate the table's data directory and extract the captured 25.8 part into
# the `detached/` subdirectory, then attach it.
DATA_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND table = 'issue_104872'")

mkdir -p "${DATA_PATH}detached/${PART_NAME}"
tar -xzf "${CUR_DIR}/data_104872/part_25.8.tar.gz" -C "${DATA_PATH}detached/${PART_NAME}"

${CLICKHOUSE_CLIENT} -q "ALTER TABLE issue_104872 ATTACH PART '${PART_NAME}'"

# Sanity check: the part is Compact, has the broken shape (column `vid`
# missing on disk), and the table reads correctly with `vid` defaulted to 0.
${CLICKHOUSE_CLIENT} -q "
SELECT part_type, name FROM system.parts
WHERE database = currentDatabase() AND table = 'issue_104872' AND active
ORDER BY name"
${CLICKHOUSE_CLIENT} -q "SELECT requestID, vid FROM issue_104872 ORDER BY requestID"

# The bug: adding a second skip index and materializing it force-recalculates
# the pre-existing `vid_ix` whose required column `vid` is not in the part on
# disk. Without the fix, the mutation fails with NOT_FOUND_COLUMN_IN_BLOCK.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE issue_104872 ADD INDEX requestID_ix requestID TYPE bloom_filter GRANULARITY 100"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE issue_104872 MATERIALIZE INDEX requestID_ix SETTINGS mutations_sync = 2"

# Verify the mutation succeeded and the data is intact.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM issue_104872"
${CLICKHOUSE_CLIENT} -q "SELECT requestID, vid FROM issue_104872 ORDER BY requestID"

# A subsequent DELETE also force-recalculates the pre-existing indices on the
# Compact part — covers the non-`MATERIALIZE INDEX` mutation entry points.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE issue_104872 DELETE WHERE requestID = 'aaa' SETTINGS mutations_sync = 2"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM issue_104872"
${CLICKHOUSE_CLIENT} -q "SELECT requestID, vid FROM issue_104872 ORDER BY requestID"

${CLICKHOUSE_CLIENT} -q "DROP TABLE issue_104872 SYNC"
