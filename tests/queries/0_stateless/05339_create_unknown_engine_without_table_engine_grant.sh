#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A user without any `TABLE ENGINE` grant gets the unknown-engine error for an unregistered engine name (no such
# grant can exist), and is still denied a registered engine. Same result whatever `table_engines_require_grant` is.

user="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_no_create="u_no_create_${CLICKHOUSE_TEST_UNIQUE_NAME}"
new_db="${CLICKHOUSE_DATABASE}_new"

trap '${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}, ${user_no_create}"; ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${new_db}"' EXIT

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}, ${user_no_create}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user_no_create}"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.* TO ${user}"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE DATABASE ON ${new_db}.* TO ${user}"

# Each arm prints a part of the server's error, never of the echoed query.
unknown_engine() { grep -oE "Unknown (table|database) engine .*\((UNKNOWN_STORAGE|UNKNOWN_DATABASE_ENGINE)\)" | head -n1; }
error_code() { grep -oE "\((UNKNOWN_STORAGE|UNKNOWN_DATABASE_ENGINE|ACCESS_DENIED)\)" | head -n1 | tr -d '()'; }
engine_denied() { grep -oE "TABLE ENGINE ON [A-Za-z0-9]+\. \(ACCESS_DENIED\)" | head -n1; }

run_as_user() { ${CLICKHOUSE_CLIENT} --user "${user}" -q "$1" 2>&1; }

echo -e "memory\t$(run_as_user "CREATE TABLE ${CLICKHOUSE_DATABASE}.t_memory (x Int32) ENGINE = memory" | unknown_engine)"
echo -e "s3\t$(run_as_user "CREATE TABLE ${CLICKHOUSE_DATABASE}.t_s3 (x Int32) ENGINE = s3('http://127.0.0.1:1/bucket/x.csv', 'CSV')" | error_code)"
echo -e "atomic\t$(run_as_user "CREATE DATABASE ${new_db} ENGINE = atomic" | unknown_engine)"
echo -e "URL\t$(run_as_user "CREATE TABLE ${CLICKHOUSE_DATABASE}.t_url (x Int32) ENGINE = URL('http://127.0.0.1:1/x.csv', 'CSV')" | engine_denied)"
echo -e "Filesystem\t$(run_as_user "CREATE DATABASE ${new_db} ENGINE = Filesystem" | engine_denied)"
echo -e "File\t$(run_as_user "CREATE TABLE ${CLICKHOUSE_DATABASE}.t_file (x Int32) ENGINE = File(CSV)" | engine_denied)"

# A user holding the grant keeps the IF NOT EXISTS no-op for an existing table or database.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.t_exists (x Int32) ENGINE = Memory"
ok_or_code() { local out; if out=$(${CLICKHOUSE_CLIENT} -q "$1" 2>&1); then echo OK; else echo "${out}" | error_code; fi; }
echo -e "exists_table\t$(ok_or_code "CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DATABASE}.t_exists (x Int32) ENGINE = memory")"
echo -e "exists_database\t$(ok_or_code "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DATABASE} ENGINE = atomic")"

# A missing grant that is checked before `TABLE ENGINE` is still the one reported.
denied_grant() { local out; out=$(cat); echo "$(grep -oE 'grant [A-Z ]+ ON' <<< "${out}" | head -n1 | sed -E 's/^grant (.*) ON$/\1/') $(error_code <<< "${out}")"; }
echo -e "no_create_table\t$(${CLICKHOUSE_CLIENT} --user "${user_no_create}" -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.t_no_create (x Int32) ENGINE = memory" 2>&1 | denied_grant)"
echo -e "no_create_database\t$(${CLICKHOUSE_CLIENT} --user "${user_no_create}" -q "CREATE DATABASE ${new_db} ENGINE = atomic" 2>&1 | denied_grant)"
echo -e "no_cluster\t$(run_as_user "CREATE DATABASE ${new_db} ON CLUSTER test_shard_localhost ENGINE = atomic" | denied_grant)"
echo -e "no_cluster_legacy_table\t$(${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_entry_format_version 2 -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.t_on_cluster ON CLUSTER test_shard_localhost (x Int32) ENGINE = memory" 2>&1 | denied_grant)"
# The current-format `CREATE TABLE ... ON CLUSTER` checks `TABLE ENGINE` before `CLUSTER`.
echo -e "no_cluster_table\t$(run_as_user "CREATE TABLE ${CLICKHOUSE_DATABASE}.t_on_cluster ON CLUSTER test_shard_localhost (x Int32) ENGINE = memory" | unknown_engine)"
${CLICKHOUSE_CLIENT} -q "GRANT CLUSTER ON *.* TO ${user}"
echo -e "on_cluster\t$(run_as_user "CREATE DATABASE ${new_db} ON CLUSTER test_shard_localhost ENGINE = atomic" | unknown_engine)"
