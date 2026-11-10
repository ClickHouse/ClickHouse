#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiline -q """
  SET enable_analyzer=1;

  CREATE TABLE t0 (c Int32 DEFAULT 7) ENGINE = MergeTree() ORDER BY tuple();
  INSERT INTO TABLE FUNCTION file(database() || '.test.bin', RowBinary) SELECT (5*number)::Int32 FROM numbers(3);

  SELECT '-- infer read schema in create as select';
  CREATE TABLE test_as_select (a Int32) Engine=Memory AS
      SELECT * FROM file(database() || '.test.bin', RowBinary);
  SELECT * FROM test_as_select ORDER BY ALL;

  SELECT '-- infer read schema in insert-select into table function';
  INSERT INTO TABLE FUNCTION remote('localhost:9000', database(), 't0', 'default', '')
      SELECT * FROM file(database() || '.test.bin', RowBinary);
  SELECT * FROM t0 ORDER BY ALL;

  SELECT '-- infer read schema in explain pipeline insert';
"""

# Check that there is no exception
$CLICKHOUSE_CLIENT -q """
  SET enable_analyzer=1;
  EXPLAIN PIPELINE INSERT INTO t0 SELECT * FROM file(database() || '.test.bin', RowBinary)
""" | grep digraph
