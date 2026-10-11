#!/usr/bin/env bash
# Tags: zookeeper
# Renaming or exchanging a dictionary, with or without the DICTIONARY keyword, requires DROP DICTIONARY on its old name
# and CREATE DICTIONARY on its new name: the table privileges, which imply the view ones, are not enough.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
partial="partial_${CLICKHOUSE_TEST_UNIQUE_NAME}"
drop_less="drop_less_${CLICKHOUSE_TEST_UNIQUE_NAME}"
create_less="create_less_${CLICKHOUSE_TEST_UNIQUE_NAME}"
db="atomic_${CLICKHOUSE_DATABASE}"
repl_db="repl_${CLICKHOUSE_DATABASE}"
lazy_db="lazy_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query "
CREATE DATABASE ${db} ENGINE = Atomic;
CREATE DATABASE ${repl_db} ENGINE = Replicated('/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/repl', '1', '1');
CREATE TABLE ${db}.t (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE VIEW ${db}.v AS SELECT 1 AS k;
CREATE DICTIONARY ${db}.d (k UInt64) PRIMARY KEY k SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0);
CREATE DICTIONARY ${db}.d2 (k UInt64) PRIMARY KEY k SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0);
CREATE DICTIONARY ${repl_db}.d (k UInt64) PRIMARY KEY k SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0);
CREATE DATABASE ${lazy_db} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE DICTIONARY ${lazy_db}.src (k UInt64) PRIMARY KEY k SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0);
CREATE TABLE ${lazy_db}.w (k UInt64) ENGINE = Dictionary(${lazy_db}.src);
DETACH DATABASE ${lazy_db};
ATTACH DATABASE ${lazy_db};
CREATE USER ${user} IDENTIFIED WITH plaintext_password BY '${user}';
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE ON ${db}.* TO ${user};
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE ON ${repl_db}.* TO ${user};
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE ON ${lazy_db}.* TO ${user};
GRANT CLUSTER ON *.* TO ${user};
CREATE USER ${partial} IDENTIFIED WITH plaintext_password BY '${partial}';
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE ON ${db}.* TO ${partial};
GRANT DROP DICTIONARY ON ${db}.d2 TO ${partial};
GRANT CREATE DICTIONARY, DROP DICTIONARY ON ${db}.tmp TO ${partial};
CREATE USER ${drop_less} IDENTIFIED WITH plaintext_password BY '${drop_less}';
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, CREATE DICTIONARY ON ${db}.* TO ${drop_less};
CREATE USER ${create_less} IDENTIFIED WITH plaintext_password BY '${create_less}';
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, DROP DICTIONARY ON ${db}.* TO ${create_less};
"

# Runs a query as a user (extra client options after it) and prints ACCESS_DENIED, BAD_ARGUMENTS, OK, or the output.
function run()
{
    local user=$1 query=$2
    shift 2
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "$user" --password "$user" --distributed_ddl_output_mode=none "$@" --query "$query" 2>&1)
    if grep -qF ACCESS_DENIED <<< "$out"; then echo ACCESS_DENIED
    elif grep -qF BAD_ARGUMENTS <<< "$out"; then echo BAD_ARGUMENTS
    elif [ -z "$out" ]; then echo OK
    else echo "$out"; fi
}

echo "-- table privileges do not move a dictionary"
run "$user" "RENAME DICTIONARY ${db}.d TO ${db}.d_moved"
run "$user" "RENAME TABLE ${db}.d TO ${db}.d_moved"
run "$user" "EXCHANGE DICTIONARIES ${db}.d AND ${db}.d2"
run "$user" "EXCHANGE TABLES ${db}.d AND ${db}.t"
run "$user" "EXCHANGE TABLES ${db}.t AND ${db}.d"
run "$user" "RENAME TABLE ${repl_db}.d TO ${repl_db}.d_moved"
run "$user" "EXCHANGE TABLES ${db}.t AND ${db}.d ON CLUSTER test_shard_localhost"
run "$user" "RENAME TABLE ${lazy_db}.w TO ${lazy_db}.w_moved"

echo "-- each side of a move needs its own dictionary privilege"
run "$drop_less" "RENAME TABLE ${db}.d TO ${db}.d_moved"
run "$create_less" "RENAME TABLE ${db}.d TO ${db}.d_moved"
run "$drop_less" "EXCHANGE TABLES ${db}.t AND ${db}.d"
run "$create_less" "EXCHANGE TABLES ${db}.t AND ${db}.d"

echo "-- on a cluster, a name this host does not have may be a dictionary on another host"
run "$user" "RENAME TABLE ${db}.absent TO ${db}.y ON CLUSTER test_shard_localhost"

echo "-- a JSON query cannot set the interpreter-only mode that turns a rename into an exchange"
json=$(${CLICKHOUSE_CLIENT} --query "
SELECT replaceOne(replaceOne(parseQueryToJSON('RENAME TABLE ${db}.t TO ${db}.d'),
    '\"rename_if_cannot_exchange\":false,', ''),
    '\"type\":\"RenameQuery\",', '\"type\":\"RenameQuery\",\"rename_if_cannot_exchange\":true,')")
run "$user" "$json" --enable_json_ast_dialect 1 --dialect clickhouse_json

echo "-- an earlier element of the query decides what a later one moves"
run "$partial" "RENAME TABLE ${db}.d2 TO ${db}.tmp, ${db}.tmp TO ${db}.x"
run "$partial" "RENAME TABLE ${db}.d2 TO ${db}.tmp"

echo "-- an element skipped by IF EXISTS leaves its target as it was"
run "$user" "RENAME TABLE ${db}.t TO ${db}.t1, IF EXISTS ${db}.t TO ${db}.d, ${db}.d TO ${db}.z"

echo "-- table privileges still move tables and views"
run "$user" "RENAME TABLE ${db}.t TO ${db}.t_moved"
run "$user" "RENAME TABLE ${db}.v TO ${db}.v_moved ON CLUSTER test_shard_localhost"

echo "-- dictionary privileges move a dictionary"
${CLICKHOUSE_CLIENT} --query "GRANT CREATE DICTIONARY, DROP DICTIONARY ON ${db}.* TO ${user}"
run "$user" "RENAME DICTIONARY ${db}.d TO ${db}.d_moved"

${CLICKHOUSE_CLIENT} --query "
SELECT if(database = '${db}', 'atomic', 'replicated'), name, engine FROM system.tables
WHERE database IN ('${db}', '${repl_db}') ORDER BY database = '${db}' DESC, name;
DROP DATABASE ${lazy_db};
DROP DATABASE ${db};
DROP DATABASE ${repl_db};
DROP USER ${user}, ${partial}, ${drop_less}, ${create_less};
"
