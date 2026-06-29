#!/usr/bin/env bash
# Tags: no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db="$CLICKHOUSE_DATABASE"
db_2="${db}_2"
backup_name="${db}_backup"

${CLICKHOUSE_CLIENT} "
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS mv;
CREATE TABLE src(Timestamp DateTime64(9), c1 String, c2 String) ENGINE=MergeTree ORDER BY Timestamp;
"

${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW mv(timestamp DateTime, c12 Nullable(String)) ENGINE=MergeTree ORDER BY timestamp AS SELECT Timestamp as timestamp, c1 || c2 as c12 FROM src"

${CLICKHOUSE_CLIENT} -q "INSERT INTO src SELECT '2024-02-22'::DateTime + number, number, number FROM numbers(3)"

echo 'mv before:'
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${db}.mv ORDER BY timestamp FORMAT TSVWithNamesAndTypes"

${CLICKHOUSE_CLIENT} -q "ALTER TABLE mv MODIFY QUERY SELECT Timestamp as timestamp, c1 || c2 as c12 FROM src"

echo
${CLICKHOUSE_CLIENT} -q "BACKUP DATABASE $db TO Disk('backups', '${backup_name}')" | grep -o "BACKUP_CREATED"
${CLICKHOUSE_CLIENT} -q "RESTORE DATABASE $db AS ${db_2} FROM Disk('backups', '${backup_name}')" | grep -o "RESTORED"

echo $'\nmv after:'
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${db_2}.mv ORDER BY timestamp FORMAT TSVWithNamesAndTypes"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${db_2}"
