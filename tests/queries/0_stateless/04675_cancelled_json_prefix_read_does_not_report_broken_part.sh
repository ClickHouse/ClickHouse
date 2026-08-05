#!/usr/bin/env bash
# Tags: long, zookeeper, no-fasttest, no-shared-merge-tree
# Tag long: only the flaky check caps a single run at 180s, and this fixture builds and scans two
#   parts of 100000 JSON keys, which exceeds that cap once several copies of it run at once.
# Tag zookeeper: parts_to_check and the part check thread only exist on a replicated table.
# Tag no-fasttest: the Fast test job runs with --timeout 60, which this fixture exceeds.
# Tag no-shared-merge-tree: the oracle counts ReplicatedMergeTreePartCheckThread log lines, so under
#   --replace-replicated-with-shared it would read 0 whether or not the guard works. Same tag as the
#   other part-check tests (04603, 04604).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Cancelling a SELECT while it is inside an Object/JSON structure-prefix read must not raise a
# suspicion that the part is broken. The readers' catch-all reports a part broken for any
# non-retryable failure, and a cancellation says nothing about the part's health.
#
# Both part types are covered because the two readers carry the suppression independently:
# MergeTreeReaderCompactSingleBuffer::readRows and MergeTreeReaderWide::readRows.
#
# The observable is the part check thread's own log line rather than system.replicas.parts_to_check:
# parts_to_check is the live queue length, so it drains as the check thread runs and reads 0 even
# on a build that did report the part. The thread's logger is named after the table, so text_log
# gives a durable per-table count.

for part_type in compact wide; do
    if [ "$part_type" = "compact" ]; then
        wide_threshold=1000000000
    else
        wide_threshold=0
    fi

    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_${part_type} SYNC"
    # object_serialization_version is pinned because the runner randomizes it between v2 and v3, and
    # under v2 both the write and the read side ignore object_shared_data_serialization_version, so the
    # ADVANCED per-granule structure prefix this exercises would not be read at all.
    #
    # object_shared_data_serialization_version_for_zero_level_parts is what makes a single INSERT
    # produce an ADVANCED part: MergeTreeIOSettings picks that setting instead of
    # object_shared_data_serialization_version whenever the part is zero level.
    #
    # index_granularity is pinned to 1 so each row is its own granule and the per-granule structure
    # prefix is read once per row; the runner otherwise randomizes it up to 65536.
    ${CLICKHOUSE_CLIENT} -q "
        CREATE TABLE t_${part_type} (id UInt64, j JSON)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_${part_type}', 'r1')
        ORDER BY id
        SETTINGS min_bytes_for_wide_part = ${wide_threshold},
                 min_rows_for_wide_part = ${wide_threshold},
                 index_granularity = 1,
                 object_serialization_version = 'v3',
                 object_shared_data_serialization_version = 'advanced',
                 object_shared_data_serialization_version_for_zero_level_parts = 'advanced'"

    # Each row carries many distinct JSON keys, so almost all of them land in shared data and the
    # per-granule structure prefix becomes large enough for a read of it to span a cancellation.
    #
    # The JSON is built inline rather than in a scalar subquery: the old analyzer does not resolve an
    # outer column inside one, so the subquery form fails there with UNKNOWN_IDENTIFIER.
    ${CLICKHOUSE_CLIENT} -q "
        INSERT INTO t_${part_type} SELECT number, toJSONString(mapFromArrays(
            arrayMap(x -> concat('k', toString(number), '_', toString(x)), range(20000)),
            arrayMap(x -> toString(x), range(20000))))::JSON FROM numbers(5)"

    # Two things are asserted about the part, and each guards a different way of losing coverage.
    #
    # The STORED shared-data serialization, because 'a part exists' holds under every shared-data
    # version and so would pass even on a run that never enters the function under test.
    # The object_shared_data.copy.* substreams are added by
    # SerializationObjectSharedData::enumerateStreams only for ADVANCED, so they are exactly the
    # discriminator (measured: 1 under v3+advanced, 0 under v2 and under map/map_with_buckets).
    #
    # The part type, because the two readers carry the suppression independently, so if a threshold
    # change or a randomizer made both cells produce the same part type, one reader would silently
    # stop being covered.
    echo "${part_type} advanced shared data: $(${CLICKHOUSE_CLIENT} -q "
        SELECT countIf(s LIKE '%object_shared_data.copy.%') > 0
        FROM system.parts_columns
        ARRAY JOIN substreams AS s
        WHERE database = currentDatabase() AND table = 't_${part_type}' AND active
          AND column = 'j'")"
    echo "${part_type} part type: $(${CLICKHOUSE_CLIENT} -q "
        SELECT DISTINCT part_type FROM system.parts
        WHERE database = currentDatabase() AND table = 't_${part_type}' AND active")"

    # A budget only lands inside the structure-prefix read while that read is still running, so the
    # useful range starts after the work preceding it and ends when it does. Both bounds are
    # wall-clock, so a fixed millisecond budget only holds at one machine speed: on a sanitizer build
    # under concurrency it expires before the read is reached, every attempt misses, and the in-reader
    # assertion below fails even though the guard is present and working.
    #
    # Hence the budget is a fraction of this fixture's own read cost, timed here rather than assumed.
    # The cost differs per part type by roughly an order of magnitude, so it is timed per cell.
    # The scan is timed by the server via query_log rather than by clock reads around it: with the
    # old analyzer a subquery whose result is unused is removed before execution, so any pair of
    # in-query clock reads measures nothing and every budget collapses to the floor.
    # The logging settings are pinned rather than assumed: without a finish row the fallback would
    # be the floor again, which is the very thing being fixed, so a missing row prints 0 and the
    # assertion below reports it instead of the run passing on an uncalibrated budget.
    time_read_ms() {
        local qid="04675-${part_type}-$$-${RANDOM}"
        ${CLICKHOUSE_CLIENT} --query_id="${qid}" --log_queries=1 --log_queries_probability=1 \
            --log_queries_min_query_duration_ms=0 --log_queries_min_type='QUERY_START' -q "
            SELECT count() FROM t_${part_type} WHERE length(JSONAllPaths(j)) > 0" > /dev/null
        ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
        ${CLICKHOUSE_CLIENT} -q "
            SELECT toUInt64(max(query_duration_ms)) FROM system.query_log
            WHERE current_database = currentDatabase()
              AND query_id = '${qid}' AND type = 'QueryFinish'"
    }
    read_ms=$(time_read_ms)
    echo "${part_type} read cost measured: $([ -n "${read_ms}" ] && [ "${read_ms}" -gt 0 ] 2>/dev/null && echo 1 || echo 0)"
    [ -n "${read_ms}" ] && [ "${read_ms}" -gt 0 ] 2>/dev/null || read_ms=1

    cancelled=0
    in_reader=0
    # Fractions from just inside the read's start to short of its end, retried because an individual
    # attempt still races the scheduler, and every cancellation on the way must leave the part
    # unsuspected. The measurement is refreshed every 8 attempts so that a run whose speed changed
    # after the first timing - a sanitizer build sharing the machine with 17 other copies - converges
    # instead of retrying a stale budget 40 times. Stopping after a fixed count would let the
    # assertion below pass vacuously on a run where none landed inside the reader.
    for attempt in $(seq 1 40); do
        # A refresh that returns no row keeps the previous measurement rather than replacing it
        # with an empty string, which would make the budget expression below a syntax error.
        if [ "$((attempt % 8))" = 0 ]; then
            refreshed=$(time_read_ms)
            if [ -n "${refreshed}" ] && [ "${refreshed}" -gt 0 ] 2>/dev/null; then read_ms=${refreshed}; fi
        fi
        budget=$(${CLICKHOUSE_CLIENT} -q "
            SELECT greatest(0.002, ${read_ms} / 1000 * (0.10 + 0.05 * (${attempt} % 5)))")
        err=$(${CLICKHOUSE_CLIENT} --max_execution_time "${budget}" -q "
                SELECT count() FROM t_${part_type} WHERE length(JSONAllPaths(j)) > 0" 2>&1 >/dev/null) && continue
        cancelled=$((cancelled + 1))
        # The reader adds this to the message only when the exception passed through its own
        # catch-all, i.e. the cancellation really was raised inside the part read.
        case "$err" in *"while reading"*) in_reader=$((in_reader + 1)); break ;; esac
    done

    # Guards against a vacuous run: without a cancellation, and without one landing inside the
    # reader, the assertion below would hold on any build.
    echo "${part_type} cancelled at least once: $([ "${cancelled}" -gt 0 ] && echo 1 || echo 0)"
    echo "${part_type} cancelled inside the reader at least once: $([ "${in_reader}" -gt 0 ] && echo 1 || echo 0)"

    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"

    # 'Enqueueing%' is what makes this an assertion rather than a race. reportBroken only appends to
    # the check thread's queue and wakes it; 'Checking part%' is logged later, from the background
    # thread, so counting only that measures whether the pool got round to running before the flush -
    # and it fails permissively, reading 0 on a build that did report the part. The enqueue is
    # synchronous with reportBroken, is logged on the same per-table logger, and every enqueue path
    # goes through that one line, so it cannot miss a report or count anything else.
    echo "${part_type} part check reports raised: $(${CLICKHOUSE_CLIENT} -q "
        SELECT count() FROM system.text_log
        WHERE logger_name = concat(currentDatabase(), '.t_${part_type} (ReplicatedMergeTreePartCheckThread)')
          AND (message LIKE 'Enqueueing%' OR message LIKE 'Checking part%')")"

    # The part must also still be readable: a cancelled prefix read must not have detached or
    # invalidated anything.
    echo "${part_type} rows after cancellations: $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_${part_type}")"

    ${CLICKHOUSE_CLIENT} -q "DROP TABLE t_${part_type} SYNC"
done
