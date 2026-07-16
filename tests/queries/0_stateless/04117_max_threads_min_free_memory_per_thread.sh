#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test for `max_threads_min_free_memory_per_thread` and
# `max_insert_threads_min_free_memory_per_thread`.
#
# Use `clickhouse-local` with a constrained `max_server_memory_usage` so the
# free-memory limiter actually kicks in, then count parallel stages from the
# `EXPLAIN PIPELINE` output.

CONFIG_FILE=$(mktemp -p "${CLICKHOUSE_TMP:-.}" 04117_config.XXXXXX.xml)
trap 'rm -f "$CONFIG_FILE"' EXIT

cat > "$CONFIG_FILE" <<'EOF'
<clickhouse>
    <max_server_memory_usage>4G</max_server_memory_usage>
</clickhouse>
EOF

run_local() {
    ${CLICKHOUSE_LOCAL} --config-file "$CONFIG_FILE" "$@"
}

# `EXPLAIN PIPELINE SELECT ...` produces text output where the source stage
# `NumbersRange` appears with an optional `× N` multiplier. Print N (or 1 if
# absent — single-threaded).
source_stream_count() {
    local out
    out=$(grep -E 'NumbersRange' | head -n1 | grep -oE '× [0-9]+' | grep -oE '[0-9]+')
    if [[ -z "$out" ]]; then echo 1; else echo "$out"; fi
}

# `EXPLAIN PIPELINE INSERT ...` produces a digraph; count distinct sink labels
# (one per insert thread).
count_labels() {
    local label="$1"
    grep -oE "${label}_[0-9]+" | sort -u | wc -l
}

# Maximum `× N` multiplier across the whole pipeline output. Reflects the
# pipeline-wide thread cap (`query_plan.setMaxThreads`), not just the source.
max_pipeline_parallelism() {
    awk 'BEGIN{m=1} match($0, /× [0-9]+/) {n=substr($0, RSTART+2, RLENGTH-2)+0; if (n>m) m=n} END{print m}'
}

for analyzer in 0 1; do
    echo "=== enable_analyzer=${analyzer} ==="

    echo "-- SELECT, limiter disabled --"
    run_local --enable_analyzer "$analyzer" --query "
        EXPLAIN PIPELINE
        SELECT count() FROM numbers_mt(1000000)
        SETTINGS max_threads = 8, max_threads_min_free_memory_per_thread = 0
    " | source_stream_count

    echo "-- SELECT, limiter 16Gi: expected 1 --"
    run_local --enable_analyzer "$analyzer" --query "
        EXPLAIN PIPELINE
        SELECT count() FROM numbers_mt(1000000)
        SETTINGS max_threads = 8, max_threads_min_free_memory_per_thread = '16Gi'
    " | source_stream_count

    echo "-- UNION ALL, limiter disabled --"
    run_local --enable_analyzer "$analyzer" --query "
        EXPLAIN PIPELINE
        SELECT * FROM numbers_mt(1000000) UNION ALL SELECT * FROM numbers_mt(1000000)
        SETTINGS max_threads = 8, max_threads_min_free_memory_per_thread = 0
    " | source_stream_count

    echo "-- UNION ALL, limiter 16Gi: expected 1 --"
    run_local --enable_analyzer "$analyzer" --query "
        EXPLAIN PIPELINE
        SELECT * FROM numbers_mt(1000000) UNION ALL SELECT * FROM numbers_mt(1000000)
        SETTINGS max_threads = 8, max_threads_min_free_memory_per_thread = '16Gi'
    " | source_stream_count

    echo "-- INTERSECT, limiter disabled --"
    run_local --enable_analyzer "$analyzer" --query "
        EXPLAIN PIPELINE
        SELECT * FROM numbers_mt(1000000) INTERSECT SELECT * FROM numbers_mt(1000000)
        SETTINGS max_threads = 8, max_threads_min_free_memory_per_thread = 0
    " | source_stream_count

    echo "-- INTERSECT, limiter 16Gi: expected 1 --"
    run_local --enable_analyzer "$analyzer" --query "
        EXPLAIN PIPELINE
        SELECT * FROM numbers_mt(1000000) INTERSECT SELECT * FROM numbers_mt(1000000)
        SETTINGS max_threads = 8, max_threads_min_free_memory_per_thread = '16Gi'
    " | source_stream_count

    echo "-- INSERT SELECT, both limiters disabled: expected 8 sinks --"
    run_local --enable_analyzer "$analyzer" --query "
        EXPLAIN PIPELINE
        INSERT INTO FUNCTION null('x UInt64') SELECT * FROM numbers_mt(1000000)
        SETTINGS max_threads = 8, max_insert_threads = 8,
                 max_threads_min_free_memory_per_thread = 0,
                 max_insert_threads_min_free_memory_per_thread = 0
    " | count_labels "NullSinkToStorage"

    echo "-- INSERT SELECT, insert limiter 16Gi: expected 1 sink --"
    run_local --enable_analyzer "$analyzer" --query "
        EXPLAIN PIPELINE
        INSERT INTO FUNCTION null('x UInt64') SELECT * FROM numbers_mt(1000000)
        SETTINGS max_threads = 8, max_insert_threads = 8,
                 max_threads_min_free_memory_per_thread = 0,
                 max_insert_threads_min_free_memory_per_thread = '16Gi'
    " | count_labels "NullSinkToStorage"

    echo "-- GROUP BY, limiter disabled: pipeline-wide parallelism > 1 --"
    run_local --enable_analyzer "$analyzer" --query "
        EXPLAIN PIPELINE
        SELECT number % 100 AS k, count() FROM numbers_mt(1000000) GROUP BY k
        SETTINGS max_threads = 8, max_threads_min_free_memory_per_thread = 0
    " | max_pipeline_parallelism

    # Regression for the pipeline thread cap path (`query_plan.setMaxThreads` in
    # `PlannerJoinTree`): when free memory is exhausted, the cap must apply to
    # downstream pipeline stages too, not just to source streams.
    echo "-- GROUP BY, limiter 16Gi: pipeline-wide parallelism capped to 1 --"
    run_local --enable_analyzer "$analyzer" --query "
        EXPLAIN PIPELINE
        SELECT number % 100 AS k, count() FROM numbers_mt(1000000) GROUP BY k
        SETTINGS max_threads = 8, max_threads_min_free_memory_per_thread = '16Gi'
    " | max_pipeline_parallelism
done
