#!/usr/bin/env bash
# `CREATE OR REPLACE` builds the new table under an internal `_tmp_replace_*` name and publishes it with a
# RENAME/EXCHANGE. That name is random, so no grant can ever cover it: everything the user is authorized for
# must be authorized against the user-visible names. Table-scoped grants on the final name (plus `SELECT` on
# the sources) must therefore be sufficient, and the grants required must be those of the object's own kind:
# replacing a view must not demand `CREATE TABLE` / `INSERT` on the view.
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
CREATE TABLE t (a Int32) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_replace (a Int32) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_populated (a Int32) ENGINE = MergeTree ORDER BY a;
CREATE VIEW v AS SELECT 1 AS x;
CREATE TABLE t_denied (a Int32) ENGINE = MergeTree ORDER BY a;
CREATE VIEW v_denied AS SELECT 1 AS x;

-- Table-scoped grants only: none of them can cover the internal \`_tmp_replace_*\` name.
GRANT SELECT ON ${db}.src TO ${granted};
GRANT CREATE TABLE, DROP TABLE ON ${db}.t TO ${granted};
GRANT CREATE TABLE, DROP TABLE ON ${db}.t_replace TO ${granted};
GRANT CREATE TABLE, DROP TABLE ON ${db}.t_new TO ${granted};
GRANT CREATE TABLE, DROP TABLE, INSERT ON ${db}.t_populated TO ${granted};
GRANT CREATE VIEW, DROP VIEW ON ${db}.v TO ${granted};

-- The same, minus the privilege each denied query below is supposed to be stopped by.
GRANT SELECT ON ${db}.src TO ${nogrant};
GRANT CREATE TABLE ON ${db}.t_denied TO ${nogrant};
GRANT CREATE TABLE, DROP TABLE ON ${db}.t_no_insert TO ${nogrant};
GRANT CREATE VIEW ON ${db}.v_denied TO ${nogrant};
"

granted_client=(${CLICKHOUSE_CLIENT} --user "${granted}" --password "${granted}")
nogrant_client=(${CLICKHOUSE_CLIENT} --user "${nogrant}" --password "${nogrant}")

run_granted() {
    local output
    if output=$("${granted_client[@]}" --query "$1" 2>&1); then echo "succeeded"; else echo "FAILED: ${output}"; fi
}

echo "-- [CREATE TABLE, DROP TABLE ON db.t] replacing an existing table must succeed:"
run_granted "CREATE OR REPLACE TABLE ${db}.t (a Int32, b String) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.columns WHERE database = '${db}' AND table = 't'"

echo "-- [CREATE TABLE, DROP TABLE ON db.t_new] creating a table that does not exist yet must succeed:"
run_granted "CREATE OR REPLACE TABLE ${db}.t_new (a Int32) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${db}.t_new"

echo "-- [CREATE TABLE, DROP TABLE ON db.t_replace] bare REPLACE TABLE must succeed:"
run_granted "REPLACE TABLE ${db}.t_replace (a Int32, b String) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.columns WHERE database = '${db}' AND table = 't_replace'"

echo "-- [CREATE VIEW, DROP VIEW ON db.v] replacing a view must need no table grants:"
run_granted "CREATE OR REPLACE VIEW ${db}.v AS SELECT 2 AS x"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${db}.v"

echo "-- [CREATE TABLE, DROP TABLE, INSERT ON db.t_populated] replacing with a populating SELECT must succeed:"
run_granted "CREATE OR REPLACE TABLE ${db}.t_populated ENGINE = MergeTree ORDER BY a AS SELECT a FROM ${db}.src"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${db}.t_populated"

echo "-- [no DROP TABLE grant] replacing an existing table must be denied:"
"${nogrant_client[@]}" --query "CREATE OR REPLACE TABLE ${db}.t_denied (a Int32, b String) ENGINE = MergeTree ORDER BY a" 2>&1 | grep -Fo ACCESS_DENIED | uniq
echo "-- [no DROP VIEW grant] replacing an existing view must be denied:"
"${nogrant_client[@]}" --query "CREATE OR REPLACE VIEW ${db}.v_denied AS SELECT 2 AS x" 2>&1 | grep -Fo ACCESS_DENIED | uniq
echo "-- [no INSERT grant] a populating replace must be denied:"
"${nogrant_client[@]}" --query "CREATE OR REPLACE TABLE ${db}.t_no_insert ENGINE = MergeTree ORDER BY a AS SELECT a FROM ${db}.src" 2>&1 | grep -Fo ACCESS_DENIED | uniq

echo "-- the denied queries left the replaced objects untouched and no temporary table behind:"
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM system.columns WHERE database = '${db}' AND table = 't_denied';
SELECT x FROM ${db}.v_denied;
EXISTS TABLE ${db}.t_no_insert;
SELECT count() FROM system.tables WHERE database = '${db}' AND startsWith(name, '_tmp_replace_');
"

${CLICKHOUSE_CLIENT} --query "DROP USER ${granted}, ${nogrant}"
