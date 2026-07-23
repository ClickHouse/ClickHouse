#!/usr/bin/env bash
# Tags: use-rocksdb

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# system.rocksdb must not walk a read-only Overlay facade: the facade owns no tables, and its
# iterator returns the underlying source tables, which the scan already visits through their own
# databases. Walking the facade too would report each overlay-backed EmbeddedRocksDB table a second
# time under the facade name, and - since the walk checks SHOW TABLES only on the walked database
# name - would expose the source table's RocksDB counters to a user granted only on the facade.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_SRC="db_src_${SUF}"
DB_OVL="db_ovl_${SUF}"
USER_OVL="u_ovl_${SUF}" # SHOW on the facade only, nothing on the source

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};

    CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
    CREATE TABLE ${DB_SRC}.r (key UInt32, value String) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
    INSERT INTO ${DB_SRC}.r VALUES (1, 'one');

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');

    CREATE USER ${USER_OVL} NOT IDENTIFIED;
    GRANT SHOW ON ${DB_OVL}.* TO ${USER_OVL};
    GRANT SELECT ON system.rocksdb TO ${USER_OVL};
"

echo 'The source table is listed under its own database (sanity, as the default user)'
${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 0 FROM system.rocksdb WHERE database = '${DB_SRC}' AND table = 'r'
"

echo 'No rows are listed under the facade database name'
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM system.rocksdb WHERE database = '${DB_OVL}'
"

echo 'A user granted only on the facade sees no rows for the source table at all'
${CLICKHOUSE_CLIENT} --user="${USER_OVL}" --query "
    SELECT count() FROM system.rocksdb WHERE table = 'r'
"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_SRC};
    DROP USER IF EXISTS ${USER_OVL};
"
