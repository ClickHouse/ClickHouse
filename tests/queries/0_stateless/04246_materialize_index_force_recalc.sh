#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree
#
# Regression test for ClickHouse/ClickHouse#104872.
#
# Background: PR #91980 widened the force-recalculate predicate in
# `MutateFromLogEntryTask::prepare` from `!is_full_part_storage` to
# `!is_full_wide_part`, which made Compact parts force-recalculate every
# pre-existing skip index on every mutation. The follow-up
# `splitAndModifyMutationCommands` only added columns of the
# *explicitly-materialized* index to the read set, so a pre-existing index
# over a column that is in the table metadata but absent from the part on
# disk (typical state of a part created in 25.8) raised
# `NOT_FOUND_COLUMN_IN_BLOCK` for the next mutation on the part.
#
# This test exercises the scenario in two complementary ways:
#
# 1. The user's exact reproducer from the issue, executed against current
#    master. With the fix in place every mutation always reads every
#    pre-existing index's required columns, so the sequence succeeds.
#
# 2. An adversarial scenario: a Compact part holds two skip indices over
#    two different columns, and we run a third mutation (`DELETE`,
#    `MATERIALIZE INDEX`, `MODIFY TTL`) on it. Force-recalculation now
#    touches both pre-existing indices; the read set must include every
#    column those indices reference, otherwise we would regress to the
#    same `NOT_FOUND_COLUMN_IN_BLOCK` shape.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS issue_104872 SYNC"

# ---- Part 1: user reproducer ----------------------------------------------
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE issue_104872 (timestamp DateTime, requestID String)
ENGINE = MergeTree() ORDER BY timestamp
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = '10G'
AS SELECT now(), 'aaa' UNION ALL SELECT now(), 'bbb'"

${CLICKHOUSE_CLIENT} -q "ALTER TABLE issue_104872 ADD COLUMN vid Int64"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE issue_104872 ADD INDEX vid_ix vid TYPE bloom_filter GRANULARITY 100"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE issue_104872 MATERIALIZE INDEX vid_ix SETTINGS mutations_sync = 2"

# The second MATERIALIZE INDEX is the one that fails on 25.8 -> 26.3 upgrades
# because the Compact part force-recalculates `vid_ix` whose required column
# `vid` is not in the read set. With the fix, all pre-existing index columns
# are added.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE issue_104872 ADD INDEX requestID_ix requestID TYPE bloom_filter GRANULARITY 100"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE issue_104872 MATERIALIZE INDEX requestID_ix SETTINGS mutations_sync = 2"

${CLICKHOUSE_CLIENT} -q "SELECT count() AS rows_after_user_repro FROM issue_104872"

# ---- Part 2: adversarial Compact part with multiple indices ---------------
${CLICKHOUSE_CLIENT} -q "DROP TABLE issue_104872 SYNC"

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE issue_104872 (a UInt64, b UInt64, c UInt64, d UInt64)
ENGINE = MergeTree() ORDER BY a
SETTINGS index_granularity = 4, min_bytes_for_wide_part = '10G', auto_statistics_types = ''"

${CLICKHOUSE_CLIENT} -q "INSERT INTO issue_104872 SELECT number, number, number, number FROM numbers(32)"

# Two skip indices over different columns. Both materialized on the same
# Compact part — every subsequent mutation will force-recalculate both.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE issue_104872 ADD INDEX b_ix b TYPE minmax GRANULARITY 1"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE issue_104872 MATERIALIZE INDEX b_ix SETTINGS mutations_sync = 2"

${CLICKHOUSE_CLIENT} -q "ALTER TABLE issue_104872 ADD INDEX c_ix c TYPE bloom_filter GRANULARITY 1"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE issue_104872 MATERIALIZE INDEX c_ix SETTINGS mutations_sync = 2"

# Source part is Compact and now carries skp_idx_b_ix and skp_idx_c_ix.
${CLICKHOUSE_CLIENT} -q "
SELECT part_type, name FROM system.parts WHERE table = 'issue_104872' AND active"

# Run a DELETE — it neither references `b` nor `c`, yet both pre-existing
# indices must be force-recalculated by prepare(). The read set must include
# `b` and `c` thanks to the fix.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE issue_104872 DELETE WHERE a < 4 SETTINGS mutations_sync = 2"
${CLICKHOUSE_CLIENT} -q "SELECT count() AS rows_after_delete FROM issue_104872"

# Run another MATERIALIZE INDEX over an unrelated column. Again forces
# recalculation of b_ix and c_ix.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE issue_104872 ADD INDEX d_ix d TYPE minmax GRANULARITY 1"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE issue_104872 MATERIALIZE INDEX d_ix SETTINGS mutations_sync = 2"
${CLICKHOUSE_CLIENT} -q "SELECT count() AS rows_after_third_index FROM issue_104872"

# Run MODIFY TTL — exercises the third mutation type touched by the same
# `splitAndModifyMutationCommands` path.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE issue_104872 MODIFY TTL toDateTime(a) + toIntervalYear(100) SETTINGS mutations_sync = 2"
${CLICKHOUSE_CLIENT} -q "SELECT count() AS rows_after_ttl FROM issue_104872"

# Verify all three indices still produce the expected pruning behaviour.
${CLICKHOUSE_CLIENT} -q "
SELECT count() AS rows_match_b FROM issue_104872 WHERE b = 5"
${CLICKHOUSE_CLIENT} -q "
SELECT count() AS rows_match_c FROM issue_104872 WHERE c = 6"
${CLICKHOUSE_CLIENT} -q "
SELECT count() AS rows_match_d FROM issue_104872 WHERE d = 7"

${CLICKHOUSE_CLIENT} -q "DROP TABLE issue_104872 SYNC"
