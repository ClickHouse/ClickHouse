#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: CHECK ALL TABLES walks every database on the server, so tables created and
# dropped by concurrently running tests could make the scan fail or produce unstable output.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# CHECK ALL TABLES must not walk a read-only Overlay facade: the facade owns no tables, and its
# iterator returns the underlying source tables, which the scan already visits through their own
# databases. Walking the facade too would check every overlay-backed table a second time and emit
# duplicate result rows under the source table's name.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_SRC="db_src_${SUF}"
DB_OVL="db_ovl_${SUF}"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};

    CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
    CREATE TABLE ${DB_SRC}.t (id UInt32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${DB_SRC}.t VALUES (1);

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');
"

RESULT=$(${CLICKHOUSE_CLIENT} --check_query_single_value_result=0 --query "CHECK ALL TABLES")

echo 'The source table is checked exactly once (no duplicate row through the facade)'
echo "${RESULT}" | grep "^${DB_SRC}" | cut -f1,2,4 | sed "s/^${DB_SRC}/SRC/"

echo 'No rows are reported under the facade database name'
echo "${RESULT}" | grep -c "^${DB_OVL}" || true

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
"
