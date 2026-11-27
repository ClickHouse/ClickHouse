#!/usr/bin/env bash
# `CREATE OR REPLACE` drops the replaced table after the swap, under an internal `_tmp_replace_*` name.
# The drop privilege for the replaced table's kind must be enforced before the swap: a denied query
# must leave the replaced table intact instead of failing after the replace is already committed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

granted="granted_${CLICKHOUSE_TEST_UNIQUE_NAME}"
nogrant="nogrant_${CLICKHOUSE_TEST_UNIQUE_NAME}"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE src (key String, value UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO src VALUES ('k1', 1);
CREATE DICTIONARY dict_granted (key String, value UInt64) PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE 'src')) LAYOUT(DIRECT());
CREATE DICTIONARY dict_denied (key String, value UInt64) PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE 'src')) LAYOUT(DIRECT());
DROP USER IF EXISTS ${granted}, ${nogrant};
CREATE USER ${granted} IDENTIFIED WITH plaintext_password BY '${granted}';
CREATE USER ${nogrant} IDENTIFIED WITH plaintext_password BY '${nogrant}';
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, CREATE VIEW, DROP VIEW ON ${db}.* TO ${granted}, ${nogrant};
-- The drop grant scoped to the replaced dictionary's real name must be sufficient.
GRANT DROP DICTIONARY ON ${db}.dict_granted TO ${granted};
"

echo "-- [scoped DROP DICTIONARY grant] replacing the dictionary with a view must succeed:"
${CLICKHOUSE_CLIENT} --user "${granted}" --password "${granted}" --query "CREATE OR REPLACE VIEW ${db}.dict_granted AS SELECT 42 AS x"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM dict_granted"

echo "-- [no DROP DICTIONARY grant] the replace must be denied:"
${CLICKHOUSE_CLIENT} --user "${nogrant}" --password "${nogrant}" --query "CREATE OR REPLACE VIEW ${db}.dict_denied AS SELECT 42 AS x" 2>&1 | grep -Fo ACCESS_DENIED | uniq

echo "-- [no DROP DICTIONARY grant] and the denied replace must leave the dictionary intact and no leftovers:"
${CLICKHOUSE_CLIENT} --query "
SELECT dictGet(dict_denied, 'value', 'k1');
SELECT count() FROM system.tables WHERE database = '${db}' AND startsWith(name, '_tmp_replace_');
"

${CLICKHOUSE_CLIENT} --query "DROP USER ${granted}, ${nogrant}"
