#!/usr/bin/env bash
# Tags: distributed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# traceView must render every span of a trace exactly once, whatever the shape of the
# parent links in the log:
#   - a span whose parent is not in the trace is a root (`orphan`);
#   - spans in a cycle of parent links (`cycle_a` <-> `cycle_b`) are reachable from no root,
#     they are rendered as a root and its subtree, and the cycle does not loop;
#   - a span logged twice (the same row read through two replicas of a `cluster` that
#     resolve to the same node, or a duplicated log row) is rendered once.
# The spans are written to the log directly: such shapes cannot be produced by a real query.

# Make sure the log table exists before writing to it, and use a trace id of this run only.
${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"
trace_id=$(${CLICKHOUSE_CLIENT} -q "select toString(generateUUIDv4())")

# Rows are ordered by (start_time_us, span_id) when read back: root, cycle_a, cycle_b, orphan,
# child, child again.
${CLICKHOUSE_CLIENT} -q "
    insert into system.opentelemetry_span_log
        (hostname, trace_id, span_id, parent_span_id, operation_name, kind, start_time_us, finish_time_us, finish_date, attribute)
    values
        ('h', '$trace_id',  1,   0, 'root',    'INTERNAL', 1000, 5000, today(), map()),
        ('h', '$trace_id',  2,   1, 'child',   'INTERNAL', 2000, 3000, today(), map()),
        ('h', '$trace_id',  2,   1, 'child',   'INTERNAL', 2000, 3000, today(), map()),
        ('h', '$trace_id', 10,  11, 'cycle_a', 'INTERNAL', 1000, 2000, today(), map()),
        ('h', '$trace_id', 11,  10, 'cycle_b', 'INTERNAL', 1200, 1800, today(), map()),
        ('h', '$trace_id', 20, 999, 'orphan',  'INTERNAL', 1500, 1600, today(), map())
"
${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"

echo "=== tree ==="
${CLICKHOUSE_CLIENT} -q "select span from traceView('$trace_id') format TSV"

echo "=== every replica of the cluster resolves to this node: still one row per span ==="
${CLICKHOUSE_CLIENT} -q "
    select count(), countIf(span like '%child%')
    from traceView('$trace_id', 40, 'test_cluster_two_shards')
    format TSV
"
