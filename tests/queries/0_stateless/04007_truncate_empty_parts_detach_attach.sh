#!/usr/bin/env bash
# Regression test: zombie data resurrection after TRUNCATE + DETACH + ATTACH.
#
# Bug: clearEmptyParts() in startup() calls getCoveredOutdatedParts() before
# outdated data parts are loaded asynchronously. The empty covering part b
# is wrongly dropped; background cleanup then deletes it from disk. On the
# next ATTACH, the data parts that b was protecting load as Active (zombie).
#
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t SYNC"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS min_bytes_for_wide_part = 0,
             old_parts_lifetime = 1,
             sleep_before_loading_outdated_parts_ms = 4000
"

${CLICKHOUSE_CLIENT} -q "INSERT INTO t SELECT number FROM numbers(1000)"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t SELECT number + 1000 FROM numbers(1000)"
# OPTIMIZE is required: it makes all_1_1_0 and all_2_2_0 Outdated with remove_time=T_optimize
# (a real timestamp). Without it, TRUNCATE would set remove_time=0 (Unix epoch) on those parts,
# making them always immediately eligible, so they would be deleted from disk during TRUNCATE
# itself and could never resurrect as zombies.
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t FINAL"

# TRUNCATE creates empty covering part all_1_2_2 over all_1_2_1.
# all_1_1_0, all_2_2_0 (Outdated from OPTIMIZE, remove_time=T_optimize) are not covered
# by renameAndCommitEmptyParts; their remove_time is < 1s old -> NOT eligible -> stay on disk.
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t"  # expected: 0

${CLICKHOUSE_CLIENT} -q "DETACH TABLE t"

# First ATTACH: clearEmptyParts() (BUG) drops all_1_2_2 because all_1_1_0
# and all_2_2_0 are not loaded yet (10s delay). all_1_2_2 gets remove_time=T_attach.
# Shell sleep 2s: background cleanup deletes all_1_2_2 after old_parts_lifetime=1s.
# all_1_1_0 and all_2_2_0 are not in data_parts_indexes -> background cleanup
# cannot touch them -> they remain on disk uncovered.
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t"  # expected: 0
sleep 2
${CLICKHOUSE_CLIENT} -q "DETACH TABLE t"

# Second ATTACH: all_1_2_2 absent from disk; all_1_1_0 and all_2_2_0 load as Active.
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t"  # expected: 0 (bug: returns 2000)

${CLICKHOUSE_CLIENT} -q "DROP TABLE t SYNC"
