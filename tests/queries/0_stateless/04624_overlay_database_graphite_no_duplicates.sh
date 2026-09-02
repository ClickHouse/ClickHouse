#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# system.graphite_retentions must not walk a read-only Overlay facade: the facade owns no tables, and its
# iterator returns the underlying source tables, which the scan already visits through their own
# databases. Walking the facade too would list every overlay-backed GraphiteMergeTree table twice
# in the Tables.database / Tables.table arrays (both times under the source table's own id).

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_SRC="db_src_${SUF}"
DB_OVL="db_ovl_${SUF}"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};

    CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
    CREATE TABLE ${DB_SRC}.g (key UInt32, Path String, Time DateTime('UTC'), Value Float64, Version UInt32)
        ENGINE = GraphiteMergeTree('graphite_rollup') ORDER BY key;

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');
"

echo 'The source table is listed exactly once (no duplicate entry through the facade)'
${CLICKHOUSE_CLIENT} --query "
    SELECT DISTINCT arrayCount(x -> x = '${DB_SRC}', \`Tables.database\`)
    FROM system.graphite_retentions
    WHERE has(\`Tables.database\`, '${DB_SRC}')
"

echo 'No entries are listed under the facade database name'
${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM system.graphite_retentions
    WHERE has(\`Tables.database\`, '${DB_OVL}')
"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
"
