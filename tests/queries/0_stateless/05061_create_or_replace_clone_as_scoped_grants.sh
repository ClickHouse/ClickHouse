#!/usr/bin/env bash
# Tags: no-replicated-database
# `CREATE OR REPLACE TABLE ... CLONE AS` attaches the source partitions to an internal `_tmp_replace_*`
# table before publishing it under the final name. The random temporary name cannot be covered by any grant,
# so the attach must be authorized against the name the table is published under: `ALTER DELETE` and `INSERT`
# on the final name plus `SELECT` on the source -- exactly what a plain `CREATE TABLE ... CLONE AS` requires.
# https://github.com/ClickHouse/ClickHouse/issues/90919

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

granted="granted_${CLICKHOUSE_TEST_UNIQUE_NAME}"
nogrant="nogrant_${CLICKHOUSE_TEST_UNIQUE_NAME}"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${granted}, ${nogrant};
CREATE USER ${granted} IDENTIFIED WITH plaintext_password BY '${granted}';
CREATE USER ${nogrant} IDENTIFIED WITH plaintext_password BY '${nogrant}';
GRANT TABLE ENGINE ON MergeTree TO ${granted}, ${nogrant};

CREATE TABLE src (a Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO src VALUES (1), (2), (3);
CREATE TABLE cloned (a Int32) ENGINE = MergeTree ORDER BY a;
CREATE TABLE cloned_denied (a Int32) ENGINE = MergeTree ORDER BY a;

-- Table-scoped grants only: none of them can cover the internal \`_tmp_replace_*\` name.
GRANT SELECT ON ${db}.src TO ${granted}, ${nogrant};
GRANT CREATE TABLE, DROP TABLE, INSERT, ALTER DELETE ON ${db}.cloned TO ${granted};
-- Everything except the target-side grants the attach needs.
GRANT CREATE TABLE, DROP TABLE ON ${db}.cloned_denied TO ${nogrant};
"

echo "-- [SELECT on the source, ALTER DELETE and INSERT on the target] the clone must succeed:"
if output=$(${CLICKHOUSE_CLIENT} --user "${granted}" --password "${granted}" \
    --query "CREATE OR REPLACE TABLE ${db}.cloned CLONE AS ${db}.src ENGINE = MergeTree ORDER BY a" 2>&1)
then echo "succeeded"; else echo "FAILED: ${output}"; fi
${CLICKHOUSE_CLIENT} --query "SELECT a FROM ${db}.cloned ORDER BY a"

echo "-- [no ALTER DELETE or INSERT on the target] the clone must be denied:"
${CLICKHOUSE_CLIENT} --user "${nogrant}" --password "${nogrant}" \
    --query "CREATE OR REPLACE TABLE ${db}.cloned_denied CLONE AS ${db}.src ENGINE = MergeTree ORDER BY a" 2>&1 | grep -Fo ACCESS_DENIED | uniq

echo "-- the denied clone left the table empty and no temporary table behind:"
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM ${db}.cloned_denied;
SELECT count() FROM system.tables WHERE database = '${db}' AND startsWith(name, '_tmp_replace_');
"

${CLICKHOUSE_CLIENT} --query "DROP USER ${granted}, ${nogrant}"
