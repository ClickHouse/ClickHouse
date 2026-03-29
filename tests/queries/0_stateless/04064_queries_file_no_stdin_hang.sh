#!/usr/bin/env bash
# Regression test for hanging when --queries-file has INSERT with inline data
# and stdin is an open pipe with no data/EOF.
# https://github.com/ClickHouse/ClickHouse/pull/96494

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUERIES_FILE="${CLICKHOUSE_TMP}/04064_queries_$$.sql"

cat > "$QUERIES_FILE" <<'EOF'
CREATE TABLE IF NOT EXISTS test_04064 (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO test_04064 VALUES (1), (2), (3);
SELECT sum(x) FROM test_04064;
DROP TABLE test_04064;
EOF

# Run with stdin as an open pipe (no data, no EOF) — must not hang.
# The timeout ensures we don't block forever if the bug regresses.
# Use a FIFO so stdin never gets EOF.
FIFO="${CLICKHOUSE_TMP}/04064_fifo_$$"
mkfifo "$FIFO"

# Open the FIFO for reading in background to keep it alive (no EOF).
exec 3<>"$FIFO"

timeout 30 $CLICKHOUSE_CLIENT --queries-file="$QUERIES_FILE" <&3 2>&1

exec 3>&-
rm -f "$FIFO" "$QUERIES_FILE"
