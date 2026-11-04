#!/usr/bin/env bash
# Tags: no-parallel
# Random settings limits: send_table_structure_on_insert_with_inline_data=(1, 1)
# This test exercises `INSERT INTO test_02270 FORMAT Values (24)` (inline `VALUES (24)` in the
# query string) combined with data on stdin (`echo "(42)"`), and the second case combines
# `FROM INFILE` with stdin. With `send_table_structure_on_insert_with_inline_data=0` the
# server-side inline-data parser sees both an inline `Values (24)` and an external stdin/infile
# stream and explicitly rejects with `NOT_IMPLEMENTED`: "Processing inline insert data with both
# inlined and external data (from stdin or infile) is not supported." The legacy
# client-converts-inline-data path silently picks one source (stdin in this case, matching the
# reference `42`). This test is specifically about the legacy inline+stdin/infile combination;
# pin the legacy path. The new path's `NOT_IMPLEMENTED` for this combination is a separate
# behavior question (document or support) and is independent of this test's purpose.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_02270"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test_02270 (x UInt32) ENGINE=Memory"

echo "(42)" | $CLICKHOUSE_CLIENT -q "INSERT INTO test_02270 FORMAT Values (24)"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02270 ORDER BY x"

echo "(24)" > 02270_data.values
echo "(42)" | $CLICKHOUSE_CLIENT -q "INSERT INTO test_02270 FROM INFILE '02270_data.values'"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test_02270 ORDER BY x"

$CLICKHOUSE_CLIENT -q "DROP TABLE test_02270"
rm 02270_data.values
