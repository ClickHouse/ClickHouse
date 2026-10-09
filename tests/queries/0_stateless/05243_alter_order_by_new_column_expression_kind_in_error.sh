#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A column added by the same ALTER may not be used in the new sorting key when it has a default expression.
# The error used to say "default expression" for a MATERIALIZED column too (#117541); it now names the kind.
# Using a subcolumn of such a column (here an element of a Tuple) is rejected in the same way.

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t (event_time DateTime) ENGINE = MergeTree ORDER BY event_time"

${CLICKHOUSE_CLIENT} -q "ALTER TABLE t ADD COLUMN ingest_time DateTime MATERIALIZED now(), MODIFY ORDER BY (event_time, ingest_time)" 2>&1 \
    | grep -m1 -oE 'Newly added column .* is forbidden'
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t ADD COLUMN insert_time DateTime DEFAULT now(), MODIFY ORDER BY (event_time, insert_time)" 2>&1 \
    | grep -m1 -oE 'Newly added column .* is forbidden'
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t ADD COLUMN tp Tuple(a UInt32, b String) MATERIALIZED (1, 'x'), MODIFY ORDER BY (event_time, tp.a)" 2>&1 \
    | grep -m1 -oE 'Newly added column .* is forbidden'

${CLICKHOUSE_CLIENT} -q "DROP TABLE t"
