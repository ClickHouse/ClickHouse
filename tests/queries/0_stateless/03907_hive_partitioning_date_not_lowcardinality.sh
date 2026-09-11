#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/87586
# Hive partitioning should not wrap non-String types in LowCardinality,
# which would cause SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY for Date columns.
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_DIR=${CLICKHOUSE_TMP}/$CLICKHOUSE_TEST_UNIQUE_NAME
mkdir -p "$DATA_DIR/hp=2025-09-24"

# Create a minimal CSV file
echo -e "id\n1\n2\n3" > "$DATA_DIR/hp=2025-09-24/data.csv"

$CLICKHOUSE_LOCAL -q "
SET use_hive_partitioning = 1;

-- The query should not fail with SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY
-- The type should be Date, not LowCardinality(Date)
SELECT id, hp, toTypeName(hp) FROM file('$DATA_DIR/hp=2025-09-24/data.csv') ORDER BY id;
"

rm -rf "$DATA_DIR"
