#!/usr/bin/env bash
# Tags: zookeeper
# `CREATE OR REPLACE` sent to a Replicated database or ON CLUSTER is replayed with full access, so the drop
# privilege for the replaced table's kind must be checked on the initiator, as the user, before it is sent.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
atomic_db="atomic_${CLICKHOUSE_DATABASE}"
repl_db="repl_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
CREATE DATABASE ${atomic_db} ENGINE = Atomic;
CREATE DATABASE ${repl_db} ENGINE = Replicated('/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/repl', '1', '1');
CREATE DICTIONARY ${atomic_db}.d (k UInt64) PRIMARY KEY k SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0);
CREATE DICTIONARY ${repl_db}.d (k UInt64) PRIMARY KEY k SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0);
CREATE USER ${user} IDENTIFIED WITH plaintext_password BY '${user}';
GRANT SELECT, CREATE VIEW, DROP VIEW ON ${atomic_db}.* TO ${user};
GRANT SELECT, CREATE VIEW, DROP VIEW ON ${repl_db}.* TO ${user};
GRANT CLUSTER ON *.* TO ${user};
"

${CLICKHOUSE_CLIENT} --user "${user}" --password "${user}" --query "CREATE OR REPLACE VIEW ${repl_db}.d AS SELECT 1" 2>&1 | grep -Fo ACCESS_DENIED | uniq
${CLICKHOUSE_CLIENT} --user "${user}" --password "${user}" --query "CREATE OR REPLACE VIEW ${atomic_db}.d ON CLUSTER test_shard_localhost AS SELECT 1" 2>&1 | grep -Fo ACCESS_DENIED | uniq

${CLICKHOUSE_CLIENT} --query "
SELECT engine FROM system.tables WHERE database IN ('${atomic_db}', '${repl_db}') AND name = 'd';
DROP DATABASE ${atomic_db};
DROP DATABASE ${repl_db};
DROP USER ${user};
"
