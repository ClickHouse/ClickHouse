#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A row policy filter is applied as a per-row predicate at the storage read stage. arrayJoin (and
# its case-insensitive alias unnest, including when hidden behind a SQL UDF wrapper) change the
# number of rows, which used to abort with the logical error 'column->size() == num_rows'. Such
# policies must be rejected: when created, when altered, and when applied.
# SQL UDFs are global (not database-scoped), so the name embeds $CLICKHOUSE_DATABASE to avoid
# collisions with concurrent test runs.

$CLICKHOUSE_CLIENT -q "
  DROP ROW POLICY IF EXISTS policy_with_array_join ON row_policy_table;
  DROP ROW POLICY IF EXISTS policy_with_unnest ON row_policy_table;
  DROP ROW POLICY IF EXISTS policy_with_udf ON row_policy_table;
  DROP ROW POLICY IF EXISTS valid_policy ON row_policy_table;
  DROP FUNCTION IF EXISTS ${CLICKHOUSE_DATABASE}_row_policy_arrayjoin_udf;
  DROP TABLE IF EXISTS row_policy_table;

  CREATE TABLE row_policy_table (id UInt32, value UInt32) ENGINE = MergeTree ORDER BY id;
  INSERT INTO row_policy_table VALUES (1, 10), (2, 20);

  -- Creating a policy whose filter contains arrayJoin is rejected.
  CREATE ROW POLICY policy_with_array_join ON row_policy_table FOR SELECT USING arrayJoin([1, 2]) OR (value = 0) TO ALL; -- { serverError ILLEGAL_PREWHERE }

  -- unnest is the case-insensitive alias of arrayJoin and is rejected identically.
  CREATE ROW POLICY policy_with_unnest ON row_policy_table FOR SELECT USING unnest([1, 2]) OR (value = 0) TO ALL; -- { serverError ILLEGAL_PREWHERE }
  CREATE ROW POLICY policy_with_unnest ON row_policy_table FOR SELECT USING UNNEST([1, 2]) OR (value = 0) TO ALL; -- { serverError ILLEGAL_PREWHERE }

  -- A SQL UDF that expands to arrayJoin is inlined at read time, so a policy using it is rejected too.
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_row_policy_arrayjoin_udf AS (x) -> (unnest([1, 2]) OR x = 0);
  CREATE ROW POLICY policy_with_udf ON row_policy_table FOR SELECT USING ${CLICKHOUSE_DATABASE}_row_policy_arrayjoin_udf(value) TO ALL; -- { serverError ILLEGAL_PREWHERE }
  DROP FUNCTION ${CLICKHOUSE_DATABASE}_row_policy_arrayjoin_udf;

  -- Altering a valid policy to introduce arrayJoin is rejected too; the original filter is kept.
  CREATE ROW POLICY valid_policy ON row_policy_table FOR SELECT USING value > 0 TO ALL;
  ALTER ROW POLICY valid_policy ON row_policy_table FOR SELECT USING arrayJoin([1, 2]) OR (value > 0); -- { serverError ILLEGAL_PREWHERE }
  SELECT * FROM row_policy_table ORDER BY id;
  DROP ROW POLICY valid_policy ON row_policy_table;

  -- arrayJoin scoped inside a subquery does not change the outer row count and stays allowed.
  CREATE ROW POLICY valid_policy ON row_policy_table FOR SELECT USING value IN (SELECT arrayJoin([10, 20])) TO ALL;
  SELECT * FROM row_policy_table ORDER BY id;

  DROP ROW POLICY valid_policy ON row_policy_table;
  DROP TABLE row_policy_table;
"
