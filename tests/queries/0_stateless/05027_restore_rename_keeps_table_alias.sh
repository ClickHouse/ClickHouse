#!/usr/bin/env bash
# Tags: no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

src="${CLICKHOUSE_DATABASE}_src"
renamed="${CLICKHOUSE_DATABASE}_renamed"
tables="${CLICKHOUSE_DATABASE}_tables"
backup_name="${CLICKHOUSE_DATABASE}_backup"

views="v_plain v_final v_comma v_comma_final v_join v_subquery_alias"

for d in "${src}" "${renamed}" "${tables}"
do
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${d} SYNC"
done

${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${src}"
${CLICKHOUSE_CLIENT} -nq "
CREATE TABLE ${src}.t1 (id UInt64, x String) ENGINE = ReplacingMergeTree ORDER BY id;
CREATE TABLE ${src}.t2 (id UInt64, y String) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${src}.t1 VALUES (1, 'a'), (2, 'b');
INSERT INTO ${src}.t2 VALUES (1, 'p'), (2, 'q');
CREATE VIEW ${src}.v_plain AS SELECT a.id FROM ${src}.t1 AS a;
CREATE VIEW ${src}.v_final AS SELECT a.id FROM ${src}.t1 AS a FINAL;
CREATE VIEW ${src}.v_comma AS SELECT a.id, b.y FROM ${src}.t1 AS a, ${src}.t2 AS b WHERE a.id = b.id;
CREATE VIEW ${src}.v_comma_final AS SELECT a.id, b.y FROM ${src}.t1 AS a FINAL, ${src}.t2 AS b WHERE a.id = b.id;
CREATE VIEW ${src}.v_join AS SELECT a.id, b.y FROM ${src}.t1 AS a JOIN ${src}.t2 AS b ON a.id = b.id;
CREATE VIEW ${src}.v_subquery_alias AS SELECT s.id FROM (SELECT id FROM ${src}.t1) AS s;
"

${CLICKHOUSE_CLIENT} -q "BACKUP DATABASE ${src} TO Disk('backups', '${backup_name}')" | grep -o "BACKUP_CREATED"

# Database names are unique per run, so print them as fixed placeholders.
show_body()
{
    ${CLICKHOUSE_CLIENT} -q "
        SELECT replaceAll(replaceAll(replaceOne(create_table_query, 'CREATE VIEW ${1}.${2} ', ''), '${1}', 'db'), '${src}', 'source_db')
        FROM system.tables WHERE database = '${1}' AND name = '${2}'"
}

echo "-- restore the database under a new name"
${CLICKHOUSE_CLIENT} -q "RESTORE DATABASE ${src} AS ${renamed} FROM Disk('backups', '${backup_name}')" | grep -o "RESTORED"
for v in $views
do
    echo "${v}"
    show_body "${renamed}" "${v}"
    ${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${renamed}.${v} ORDER BY id"
done

echo "-- restore the database under its original name"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${src} SYNC"
${CLICKHOUSE_CLIENT} -q "RESTORE DATABASE ${src} FROM Disk('backups', '${backup_name}')" | grep -o "RESTORED"
for v in $views
do
    echo "${v}"
    show_body "${src}" "${v}"
    ${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${src}.${v} ORDER BY id"
done

echo "-- restore a view together with the table it reads, both under new names"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${tables}"
${CLICKHOUSE_CLIENT} -q "RESTORE TABLE ${src}.t1 AS ${tables}.t1, TABLE ${src}.v_plain AS ${tables}.v_plain FROM Disk('backups', '${backup_name}')" | grep -o "RESTORED"
show_body "${tables}" "v_plain"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${tables}.v_plain ORDER BY id"

echo "-- restore a view alone, leaving the table it reads where it is"
${CLICKHOUSE_CLIENT} -q "RESTORE TABLE ${src}.v_plain AS ${tables}.v_alone FROM Disk('backups', '${backup_name}')" | grep -o "RESTORED"
show_body "${tables}" "v_alone"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${tables}.v_alone ORDER BY id"

for d in "${src}" "${renamed}" "${tables}"
do
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${d} SYNC"
done
