#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# system.s3_queue_settings / system.azure_queue_settings must not walk a read-only Overlay facade: the
# facade owns no tables, and its iterator returns the underlying source tables, which the scan already
# visits through their own databases. Walking the facade too would list every overlay-backed queue table
# twice (both times under the source table's own id), even though queue settings cannot be managed
# through the facade.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_SRC="db_src_${SUF}"
DB_OVL="db_ovl_${SUF}"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};

    CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
    CREATE TABLE ${DB_SRC}.q (column1 UInt32, column2 UInt32)
        ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/${SUF}/', 'username', 'password', CSV)
        SETTINGS mode = 'ordered', keeper_path = '/clickhouse/s3queue/${SUF}';

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');
"

echo 'Each setting of the source queue table is listed exactly once (no duplicate rows through the facade)'
${CLICKHOUSE_CLIENT} --query "
    SELECT count() = count(DISTINCT name)
    FROM system.s3_queue_settings
    WHERE database = '${DB_SRC}' AND table = 'q'
"

echo 'No queue settings are listed under the facade database name'
${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM system.s3_queue_settings
    WHERE database = '${DB_OVL}'
"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
"
