#!/usr/bin/env bash
# Regression coverage for issue #89797.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `hypothesis` index type was removed. A legacy stub allows ATTACH of old tables for
# compatibility but rejects every path that would create a new one.

# CREATE TABLE with the exact table shape from the issue is rejected.
$CLICKHOUSE_CLIENT --query "
    SET allow_suspicious_low_cardinality_types = 1;
    CREATE TABLE t0 (c0 LowCardinality(UInt128), INDEX i0 90 % c0 TYPE hypothesis, c1 Bool, c2 Map(Int64, Int64))
    ENGINE = MergeTree() PRIMARY KEY tuple();
" 2>&1 | grep -oF "no longer supported" | head -n1

# A minimal hypothesis index is rejected too, independent of column type.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_create (a UInt64, INDEX i0 90 % a TYPE hypothesis) ENGINE = MergeTree() PRIMARY KEY tuple();
" 2>&1 | grep -oF "no longer supported" | head -n1

# ALTER ADD INDEX and CREATE INDEX are rejected as well.
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_alter (a UInt64) ENGINE = MergeTree() PRIMARY KEY tuple();"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_alter ADD INDEX i0 90 % a TYPE hypothesis;" 2>&1 | grep -oF "no longer supported" | head -n1
$CLICKHOUSE_CLIENT --query "CREATE INDEX i0 ON t_alter (90 % a) TYPE hypothesis;" 2>&1 | grep -oF "no longer supported" | head -n1
$CLICKHOUSE_CLIENT --query "DROP TABLE t_alter;"

# Full-definition ATTACH keeps the compatibility bypass: it forwards attach = true, so a table
# carrying a hypothesis index is NOT rejected and can still be recreated (the metadata shape
# #89797 depends on). Atomic databases require an explicit UUID for full-definition ATTACH.
uuid=$($CLICKHOUSE_CLIENT --query "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT --query "
    SET allow_suspicious_low_cardinality_types = 1, send_logs_level = 'fatal';
    ATTACH TABLE t_attach UUID '$uuid' (c0 LowCardinality(UInt128), INDEX i0 90 % c0 TYPE hypothesis, c1 Bool)
    ENGINE = MergeTree() PRIMARY KEY tuple();
"
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_attach' AND type = 'hypothesis';
"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_attach;"
