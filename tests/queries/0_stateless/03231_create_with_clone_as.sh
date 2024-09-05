#!/usr/bin/env bash
# Tags: no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS foo_memory"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS clone_as_foo_memory"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS foo_file"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS clone_as_foo_file"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS foo_merge_tree"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS clone_as_foo_merge_tree"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS foo_replacing_merge_tree"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS clone_as_foo_replacing_merge_tree"

# CLONE AS with a table of Memory engine
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "CREATE TABLE foo_memory (x Int8, y String) ENGINE=Memory"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "SHOW CREATE TABLE foo_memory"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "INSERT INTO foo_memory VALUES (1, 'a'), (2, 'b')"

echo "$(${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --server_logs_file=/dev/null -q "CREATE TABLE clone_as_foo_memory CLONE AS foo_memory" 2>&1)" \
  | grep -c 'Code: 344. DB::Exception: .* Only support CLONE AS with tables of the MergeTree family'


# CLONE AS with a table of File engine
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "CREATE TABLE foo_file (x Int8, y String) ENGINE=File(TabSeparated)"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "SHOW CREATE TABLE foo_file"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "INSERT INTO foo_file VALUES (1, 'a'), (2, 'b')"

echo "$(${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --server_logs_file=/dev/null -q "CREATE TABLE clone_as_foo_file CLONE AS foo_file" 2>&1)" \
  | grep -c 'Code: 344. DB::Exception: .* Only support CLONE AS with tables of the MergeTree family'

# CLONE AS with a table of MergeTree engine
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "CREATE TABLE foo_merge_tree (x Int8, y String) ENGINE=MergeTree PRIMARY KEY x"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "SHOW CREATE TABLE foo_merge_tree"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "INSERT INTO foo_merge_tree VALUES (1, 'a'), (2, 'b')"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "SELECT * FROM foo_merge_tree"

${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "CREATE TABLE clone_as_foo_merge_tree CLONE AS foo_merge_tree"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "SHOW CREATE TABLE clone_as_foo_merge_tree"
echo "from foo_merge_tree"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "SELECT * FROM foo_merge_tree"
echo "from clone_as_foo_merge_tree"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "SELECT * FROM clone_as_foo_merge_tree"

# CLONE AS with a table of ReplacingMergeTree engine
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "CREATE TABLE foo_replacing_merge_tree (x Int8, y String) ENGINE=ReplacingMergeTree PRIMARY KEY x"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "SHOW CREATE TABLE foo_replacing_merge_tree"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "INSERT INTO foo_replacing_merge_tree VALUES (1, 'a'), (2, 'b')"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "SELECT * FROM foo_replacing_merge_tree"

${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "CREATE TABLE clone_as_foo_replacing_merge_tree CLONE AS foo_replacing_merge_tree"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "SHOW CREATE TABLE clone_as_foo_replacing_merge_tree"
echo "from foo_replacing_merge_tree"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "SELECT * FROM foo_replacing_merge_tree"
echo "from clone_as_foo_replacing_merge_tree"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "SELECT * FROM clone_as_foo_replacing_merge_tree FINAL"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS foo_memory"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS clone_as_foo_memory"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS foo_file"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS clone_as_foo_file"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS foo_merge_tree"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS clone_as_foo_merge_tree"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS foo_replacing_merge_tree"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS clone_as_foo_replacing_merge_tree"

# CLONE AS with a Replicated database
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS imdb_03231"

${CLICKHOUSE_CLIENT} -q "CREATE DATABASE imdb_03231 ENGINE = Replicated('/test/databases/imdb_03231', 's1', 'r1')"

${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "CREATE TABLE imdb_03231.foo_merge_tree (x Int8, y String) ENGINE=MergeTree PRIMARY KEY x"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "SHOW CREATE TABLE imdb_03231.foo_merge_tree"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "INSERT INTO imdb_03231.foo_merge_tree VALUES (1, 'a'), (2, 'b')"
echo "from imdb_03231.foo_merge_tree"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 -q "SELECT * FROM imdb_03231.foo_merge_tree"
echo "$(${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --server_logs_file=/dev/null -q "CREATE TABLE imdb_03231.clone_as_foo_merge_tree CLONE AS imdb_03231.foo_merge_tree" 2>&1)" \
  | grep -c 'Code: 344. DB::Exception: .* CREATE CLONE AS is not supported with Replicated databases. Consider using separate CREATE and INSERT queries.'

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS imdb_03231.clone_as_foo_merge_tree"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS imdb_03231.foo_merge_tree"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS imdb_03231"