#!/usr/bin/env bash
# The access contract the Mongo endpoint relies on for the probes that precede a command.
#
# A collection is answered by `find`, `count`, `distinct`, `aggregate`, `update` and `delete` only
# after the endpoint has asked whether it exists and what shape it has, and `createCollection` and
# the first `insert` create the database of a collection. Both of those questions must cost no
# privilege beyond the command they precede:
#
# - `EXISTS TABLE` / `EXISTS DATABASE` and the `system.columns` / `system.tables` rows of the
#   collection are readable for a user who only holds `SELECT` on it, because any privilege on a
#   table implies `SHOW TABLES` on it and `SHOW DATABASES` on its database.
# - `CREATE DATABASE IF NOT EXISTS`, on the other hand, is checked against `CREATE DATABASE` even
#   when the database is already there, which is why the endpoint probes before it runs it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

READER="reader_${CLICKHOUSE_DATABASE}"
WRITER="writer_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS probe_collection;
    CREATE TABLE probe_collection (\`_id\` String, \`json\` JSON) ENGINE = MergeTree ORDER BY \`_id\`
        COMMENT 'A collection of the Mongo endpoint';

    DROP USER IF EXISTS ${READER}, ${WRITER};
    CREATE USER ${READER} IDENTIFIED WITH no_password;
    CREATE USER ${WRITER} IDENTIFIED WITH no_password;

    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.probe_collection TO ${READER};
    REVOKE SHOW TABLES ON ${CLICKHOUSE_DATABASE}.probe_collection FROM ${READER};
    REVOKE SHOW DATABASES ON ${CLICKHOUSE_DATABASE}.* FROM ${READER};

    GRANT CREATE TABLE, INSERT, SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${WRITER};
"

echo '-- the grants of the reader, after the implied ones were revoked'
# The name of the database of a test differs from run to run, so it is replaced by a fixed one.
${CLICKHOUSE_CLIENT} --user "${READER}" --query "SHOW GRANTS FOR CURRENT_USER" \
    | sed -e "s/${CLICKHOUSE_DATABASE}/<database>/g"

echo '-- the existence probes of the endpoint answer for it'
${CLICKHOUSE_CLIENT} --user "${READER}" --query "EXISTS TABLE probe_collection"
${CLICKHOUSE_CLIENT} --user "${READER}" --query "EXISTS DATABASE ${CLICKHOUSE_DATABASE}"

echo '-- and so do the shape probes'
${CLICKHOUSE_CLIENT} --user "${READER}" --query "
    SELECT countIf(name = 'json' AND type = 'JSON'), countIf(name = '_id'), count()
    FROM system.columns WHERE database = currentDatabase() AND table = 'probe_collection'"
${CLICKHOUSE_CLIENT} --user "${READER}" --query "
    SELECT count() FROM system.tables
    WHERE database = currentDatabase() AND table = 'probe_collection' AND comment = 'A collection of the Mongo endpoint'"

echo '-- a user who may create a table in the database may not create the database'
# The name of the missing privilege is answered as a line of its own: the message itself names the
# database, which differs from run to run, and the test runner rejects the word `Exception` in the
# standard output of a test.
if ${CLICKHOUSE_CLIENT} --user "${WRITER}" \
        --query "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DATABASE}" 2>&1 >/dev/null \
        | grep -q 'necessary to have the grant CREATE DATABASE'
then
    echo 'the grant CREATE DATABASE is missing'
fi
echo '-- while the probe that replaces it answers'
${CLICKHOUSE_CLIENT} --user "${WRITER}" --query "EXISTS DATABASE ${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    DROP USER ${READER}, ${WRITER};
    DROP TABLE probe_collection;
"
