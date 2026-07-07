#!/usr/bin/env bash
# Tags: no-fasttest, no-encrypted-storage

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}_inner
$CLICKHOUSE_CLIENT -nm -q "
DROP DATABASE IF EXISTS ${db};
CREATE DATABASE ${db};

CREATE TABLE ${db}.test_table_1 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${db}.test_table_1 SELECT number, number FROM numbers(15000);

CREATE TABLE ${db}.test_table_2 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${db}.test_table_2 SELECT number, number FROM numbers(15000);

SELECT (id % 10) AS key, count() FROM ${db}.test_table_1 GROUP BY key ORDER BY key;

SELECT '--';

SELECT (id % 10) AS key, count() FROM ${db}.test_table_2 GROUP BY key ORDER BY key;

BACKUP DATABASE ${db} TO Disk('backups', '${db}') FORMAT Null;

SELECT '--';

DROP DATABASE IF EXISTS ${db}_backup_database;
CREATE DATABASE ${db}_backup_database ENGINE = Backup('${db}', Disk('backups', '${db}'));
SHOW CREATE DATABASE ${db}_backup_database;

SELECT name, total_rows FROM system.tables WHERE database = '${db}_backup_database' ORDER BY name;

SELECT '--';

SELECT (id % 10) AS key, count() FROM ${db}_backup_database.test_table_1 GROUP BY key ORDER BY key;

SELECT '--';

SELECT (id % 10) AS key, count() FROM ${db}_backup_database.test_table_2 GROUP BY key ORDER BY key;

DROP DATABASE ${db}_backup_database;

DROP DATABASE ${db};
"
