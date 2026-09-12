#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A TTLDrop merge takes the short-circuit that skips the read pipeline. Every index the
# resulting empty part must still carry has to be handled there, because the builders that
# normally produce them live inside the skipped pipeline.
#
# Only the background selector assigns MergeType::TTLDrop; OPTIMIZE TABLE FINAL assigns
# MergeType::Regular. Every case below therefore drives a background TTL merge and waits for
# it, bounded by wall-clock time rather than a fixed iteration count.

function wait_for_ttl_drop()
{
    local table=$1
    local deadline=$((SECONDS + 90))
    while [ "$SECONDS" -lt "$deadline" ]; do
        local rows
        rows=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM $table")
        if [ "$rows" = "0" ]; then
            return
        fi
        sleep 0.5
    done
    echo "timed out waiting for the TTL drop merge on $table"
}

echo "-- Case 1: text index"

# Before the fix the merge threw LOGICAL_ERROR 'Text index transform for index ... not found'
# and retried forever, so the expired rows were never dropped.
${CLICKHOUSE_CLIENT} -q "
    SET allow_experimental_full_text_index = 1;

    CREATE TABLE t_ttl_drop_text
    (
        id UInt64,
        value String,
        event_time DateTime DEFAULT now() - INTERVAL 2 DAY,
        INDEX idx_txt value TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY
    SETTINGS
        ttl_only_drop_parts = 1,
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 1,
        -- keep the 0-row part so the index assertions below have something to read
        remove_empty_parts = 0;

    SYSTEM STOP MERGES t_ttl_drop_text;

    INSERT INTO t_ttl_drop_text (id, value) SELECT number, 'w' || toString(number) FROM numbers(100);
    INSERT INTO t_ttl_drop_text (id, value) SELECT number, 'w' || toString(number) FROM numbers(100);

    SYSTEM START MERGES t_ttl_drop_text;
"

wait_for_ttl_drop "t_ttl_drop_text"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_drop_text;"

# The empty part must still carry the text index files, byte-for-byte as the normal 0-row
# path writes them: a replica that fetches the part compares checksums, so an index dropped
# here would diverge. data_compressed_bytes counts the serialized index header, which is
# non-empty even with no tokens -- it reads 0 when the index files are missing entirely.
${CLICKHOUSE_CLIENT} -q "
    SELECT name, type, data_compressed_bytes > 0
    FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_ttl_drop_text';
"

${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_ttl_drop_text SETTINGS check_query_single_value_result = 1;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_drop_text;"

echo "-- Case 2: materialize_skip_indexes_on_merge = 0 is respected"

# The 'forget about skip indexes' clear runs before the short-circuit, which used to put
# every index straight back -- so the setting was honoured for a normal merge and silently
# ignored for a TTLDrop merge. With a text index that also reintroduced case 1's failure.
${CLICKHOUSE_CLIENT} -q "
    SET allow_experimental_full_text_index = 1;

    CREATE TABLE t_ttl_drop_no_materialize
    (
        id UInt64,
        value String,
        event_time DateTime DEFAULT now() - INTERVAL 2 DAY,
        INDEX idx_txt value TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1,
        INDEX idx_set id TYPE set(100) GRANULARITY 1
    )
    ENGINE = MergeTree()
    ORDER BY id
    TTL event_time + INTERVAL 1 DAY
    SETTINGS
        ttl_only_drop_parts = 1,
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 1,
        materialize_skip_indexes_on_merge = 0,
        remove_empty_parts = 0;

    SYSTEM STOP MERGES t_ttl_drop_no_materialize;

    INSERT INTO t_ttl_drop_no_materialize (id, value) SELECT number, 'w' || toString(number) FROM numbers(100);
    INSERT INTO t_ttl_drop_no_materialize (id, value) SELECT number, 'w' || toString(number) FROM numbers(100);

    SYSTEM START MERGES t_ttl_drop_no_materialize;
"

wait_for_ttl_drop "t_ttl_drop_no_materialize"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_drop_no_materialize;"

# The setting suppresses both index families, so neither writes any files.
${CLICKHOUSE_CLIENT} -q "
    SELECT name, type, data_compressed_bytes > 0
    FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_ttl_drop_no_materialize'
    ORDER BY name;
"

${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_ttl_drop_no_materialize SETTINGS check_query_single_value_result = 1;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_drop_no_materialize;"

echo "-- Case 3: inert index"

# A legacy 'hypothesis' index is inert: it holds no data and cannot be recomputed, so
# createIndexAggregator rejects it with ILLEGAL_INDEX. The short-circuit used to omit the
# isInert filter the normal path applies, which wedged the merge and left the rows in place.
# The non-inert sibling pins that the filter is per index rather than per table: it must still
# materialize while the inert one is skipped.
# Full-definition ATTACH is the only way to get such a table, and it needs an explicit UUID.
uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")
${CLICKHOUSE_CLIENT} -q "
    SET send_logs_level = 'fatal';
    SET allow_experimental_full_text_index = 1;

    ATTACH TABLE t_ttl_drop_inert UUID '$uuid'
    (
        id UInt64,
        value String,
        event_time DateTime DEFAULT now() - INTERVAL 2 DAY,
        INDEX i0 90 % id TYPE hypothesis,
        INDEX idx_txt value TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
    )
    ENGINE = MergeTree()
    PRIMARY KEY tuple()
    TTL event_time + INTERVAL 1 DAY
    SETTINGS
        ttl_only_drop_parts = 1,
        merge_with_ttl_timeout = 0,
        min_bytes_for_wide_part = 1,
        remove_empty_parts = 0;
"

${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_ttl_drop_inert' AND type = 'hypothesis';
"

${CLICKHOUSE_CLIENT} -q "
    SYSTEM STOP MERGES t_ttl_drop_inert;

    INSERT INTO t_ttl_drop_inert (id, value) SELECT number, 'w' || toString(number) FROM numbers(100);
    INSERT INTO t_ttl_drop_inert (id, value) SELECT number + 100, 'w' || toString(number) FROM numbers(100);

    SYSTEM START MERGES t_ttl_drop_inert;
"

wait_for_ttl_drop "t_ttl_drop_inert"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_drop_inert;"

# The inert index is skipped individually; its non-inert sibling still gets materialized.
${CLICKHOUSE_CLIENT} -q "
    SELECT name, type, data_compressed_bytes > 0
    FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_ttl_drop_inert'
    ORDER BY name;
"

${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_ttl_drop_inert SETTINGS check_query_single_value_result = 1;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_drop_inert;"

echo "-- Case 4: default TTL settings"

# TTLDrop selection ignores merge_with_ttl_timeout: TTLPartDropMergeSelector passes a null
# merge_due_times, and selectPartsToMerge schedules no next time for MergeType::TTLDrop. So
# the short-circuit is reachable with every TTL setting left at its default, including
# ttl_only_drop_parts = 0, which the cases above do not cover.
#
# No OPTIMIZE here: with one level-0 part it would select a MergeType::Regular merge of its
# own and could empty the table through the normal pipeline before the background TTLDrop is
# picked up. Only the background selector produces the merge type this case is about.
${CLICKHOUSE_CLIENT} -q "
    SET allow_experimental_full_text_index = 1;

    CREATE TABLE t_ttl_drop_default
    (
        c0 UInt64,
        c1 Date,
        c2 String,
        INDEX idx0 c2 TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
    )
    ENGINE = MergeTree()
    ORDER BY c0
    TTL c1 + INTERVAL 1 SECOND DELETE;

    INSERT INTO t_ttl_drop_default SELECT number, toDate('2020-01-01'), 'w' || toString(number) FROM numbers(100);
"

wait_for_ttl_drop "t_ttl_drop_default"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ttl_drop_default;"

# Emptying the table is not enough on its own: assert the merge that did it was a TTLDrop
# that succeeded and read no rows. error = 0 excludes the failed-and-retried merge the
# unfixed code produced, which MergePlainMergeTreeTask also logs under this merge_reason;
# read_rows = 0 is what distinguishes the skipped pipeline from one that ran.
${CLICKHOUSE_CLIENT} -q "
    SYSTEM FLUSH LOGS part_log;
    SELECT countIf(merge_reason = 'TTLDropMerge' AND error = 0 AND read_rows = 0) > 0
    FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_drop_default' AND event_type = 'MergeParts';
"

${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_ttl_drop_default SETTINGS check_query_single_value_result = 1;"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ttl_drop_default;"
