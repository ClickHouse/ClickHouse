#!/usr/bin/env bash
# Verify that structure hint from INSERT table is propagated to table functions
# when optimize_trivial_insert_select is enabled (default).
# https://github.com/ClickHouse/ClickHouse/issues/103083

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${DIR}"

# Create a RowBinary file (RowBinary cannot auto-infer schema, requires structure hint)
${CLICKHOUSE_LOCAL} --query "SELECT (5 * number)::Int32 FROM numbers(3) FORMAT RowBinary" > "${DIR}/data.bin"

# With optimize_trivial_insert_select = 0, structure hint from insert table is propagated
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE t (c Int32 DEFAULT 7) ENGINE = MergeTree() ORDER BY tuple();
    SET optimize_trivial_insert_select = 0;
    INSERT INTO t SELECT * FROM file('${DIR}/data.bin', RowBinary);
    SELECT 'opt_off', c FROM t ORDER BY c;
"

# With optimize_trivial_insert_select = 1 (default), structure hint should also work
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE t (c Int32 DEFAULT 7) ENGINE = MergeTree() ORDER BY tuple();
    SET optimize_trivial_insert_select = 1;
    INSERT INTO t SELECT * FROM file('${DIR}/data.bin', RowBinary);
    SELECT 'opt_on', c FROM t ORDER BY c;
"

rm -rf "${DIR}"
