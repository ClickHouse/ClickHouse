#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CLICKHOUSE_TMP"

# Buffers carries no per-column serialization kind on the wire, so BuffersWriter must fully densify a
# column (convertToFullIfWrapped) before the type-level serializer sees it. A lazy-replication JOIN
# with special serialization kinds allowed in the output can hand it a ColumnReplicated. Round-trip a
# tuple<String> column produced this way through Buffers.

$CLICKHOUSE_LOCAL \
    --enable_lazy_columns_replication 1 \
    --allow_special_serialization_kinds_in_output_formats 1 \
    --query "SELECT (toString(b.k), a.x) AS t
        FROM (SELECT number AS k, repeat('a', 1 + number % 40) AS x FROM numbers(3000)) a
        RIGHT JOIN (SELECT number AS k FROM numbers(3000)) b USING (k)
        ORDER BY b.k" \
    --output-format Buffers \
    > 04513_buffers_replicated_tuple.buffers

$CLICKHOUSE_LOCAL \
    --structure 't Tuple(String, String)' \
    --input-format Buffers \
    --query "SELECT count(), sum(cityHash64(t.1, t.2)) FROM table" \
    < 04513_buffers_replicated_tuple.buffers

rm -f 04513_buffers_replicated_tuple.buffers
