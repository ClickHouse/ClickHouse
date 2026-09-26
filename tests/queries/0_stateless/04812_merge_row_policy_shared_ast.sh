#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `RowPolicyFilter::expression` is the parsed policy condition owned by `RowPolicyCache` and is shared by
# every query that reads the table. `ReadFromMerge::RowPolicyData` used to hand it straight to
# `TreeRewriter`, which rewrites the AST it is given in place and substitutes the results of scalar
# subqueries for the subqueries themselves. Reading a `Merge` table therefore froze the value of a scalar
# subquery inside the policy in the cache, for the rest of the server's lifetime and for every user.
# The same in-place rewrite was reported by ThreadSanitizer as a data race on the shared AST.
#
# This is a shell test because the table inside the policy's subquery has to be qualified with the
# database name: the policy condition is analyzed anew for every read, including reads on the remote
# side of a parallel-replicas query, where the session default database is not the test database.

$CLICKHOUSE_CLIENT -q "
  DROP TABLE IF EXISTS t_04812_src;
  DROP TABLE IF EXISTS t_04812_limit;
  DROP TABLE IF EXISTS t_04812_merge;
  DROP ROW POLICY IF EXISTS p_04812 ON t_04812_src;

  CREATE TABLE t_04812_src (x UInt64) ENGINE = MergeTree ORDER BY x;
  INSERT INTO t_04812_src SELECT number FROM numbers(10);

  CREATE TABLE t_04812_limit (v UInt64) ENGINE = MergeTree ORDER BY v;
  INSERT INTO t_04812_limit VALUES (3);

  CREATE ROW POLICY p_04812 ON t_04812_src USING x <= (SELECT max(v) FROM ${CLICKHOUSE_DATABASE}.t_04812_limit) TO ALL;
  CREATE TABLE t_04812_merge (x UInt64) ENGINE = Merge(currentDatabase(), '^t_04812_src\$');

  -- The policy admits 0, 1, 2, 3.
  SELECT count() FROM t_04812_merge;
  SELECT count() FROM t_04812_src;

  INSERT INTO t_04812_limit VALUES (7);

  -- The policy now admits 0 .. 7. Reading through Merge used to keep answering 4, and so did the direct
  -- read, because the Merge read above had replaced the subquery with its value in the cached policy AST.
  SELECT count() FROM t_04812_merge;
  SELECT count() FROM t_04812_src;

  DROP ROW POLICY p_04812 ON t_04812_src;
  DROP TABLE t_04812_merge;
  DROP TABLE t_04812_limit;
  DROP TABLE t_04812_src;
"
