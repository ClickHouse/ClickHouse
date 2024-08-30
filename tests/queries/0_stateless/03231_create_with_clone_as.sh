#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

clickhouse-client -q "DROP TABLE IF EXISTS foo_memory"
clickhouse-client -q "DROP TABLE IF EXISTS clone_as_foo_memory"
clickhouse-client -q "DROP TABLE IF EXISTS foo_file"
clickhouse-client -q "DROP TABLE IF EXISTS clone_as_foo_file"
clickhouse-client -q "DROP TABLE IF EXISTS foo_merge_tree"
clickhouse-client -q "DROP TABLE IF EXISTS clone_as_foo_merge_tree"
clickhouse-client -q "DROP TABLE IF EXISTS foo_replacing_merge_tree"
clickhouse-client -q "DROP TABLE IF EXISTS clone_as_foo_replacing_merge_tree"

# CLONE AS with a table of Memory engine
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --query="CREATE TABLE foo_memory (x Int8) ENGINE=Memory"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --query="INSERT INTO foo_memory VALUES (1), (2)"

echo "$(${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --server_logs_file=/dev/null --query="CREATE TABLE clone_as_foo_memory CLONE AS foo_memory" 2>&1)" \
  | grep -c 'Code: 344. DB::Exception: .* Only support CLONE AS with tables of the MergeTree family'


# CLONE AS with a table of File engine
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --query="CREATE TABLE foo_file (x Int8) ENGINE=File(TabSeparated)"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --query="INSERT INTO foo_file VALUES (1), (2)"

echo "$(${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --server_logs_file=/dev/null --query="CREATE TABLE clone_as_foo_file CLONE AS foo_file" 2>&1)" \
  | grep -c 'Code: 344. DB::Exception: .* Only support CLONE AS with tables of the MergeTree family'

# CLONE AS with a table of MergeTree engine
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --query="CREATE TABLE foo_merge_tree (x Int8) ENGINE=MergeTree PRIMARY KEY x"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --query="INSERT INTO foo_merge_tree VALUES (1), (2)"

${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --query="CREATE TABLE clone_as_foo_merge_tree CLONE AS foo_merge_tree"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --query="SHOW CREATE TABLE clone_as_foo_merge_tree"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --query="SELECT * FROM foo_merge_tree"

# CLONE AS with a table of ReplacingMergeTree engine
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --query="CREATE TABLE foo_replacing_merge_tree (x Int8) ENGINE=ReplacingMergeTree PRIMARY KEY x"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --query="INSERT INTO foo_replacing_merge_tree VALUES (1), (2)"

${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --query="CREATE TABLE clone_as_foo_replacing_merge_tree CLONE AS foo_replacing_merge_tree"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --query="SHOW CREATE TABLE clone_as_foo_replacing_merge_tree"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --query="SELECT * FROM clone_as_foo_replacing_merge_tree"


clickhouse-client -q "DROP TABLE IF EXISTS foo_memory"
clickhouse-client -q "DROP TABLE IF EXISTS clone_as_foo_memory"
clickhouse-client -q "DROP TABLE IF EXISTS foo_file"
clickhouse-client -q "DROP TABLE IF EXISTS clone_as_foo_file"
clickhouse-client -q "DROP TABLE IF EXISTS foo_merge_tree"
clickhouse-client -q "DROP TABLE IF EXISTS clone_as_foo_merge_tree"
clickhouse-client -q "DROP TABLE IF EXISTS foo_replacing_merge_tree"
clickhouse-client -q "DROP TABLE IF EXISTS clone_as_foo_replacing_merge_tree"