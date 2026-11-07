#!/usr/bin/env bash

# An ALIAS column must not read itself through a subcolumn hidden in a SQL user-defined function
# of a table function structure.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Without prefer_column_name_to_alias the default expression validation already fails with `Recursive lambda`.
$CLICKHOUSE_CLIENT --prefer_column_name_to_alias 1 -q "
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_self AS () -> a.size;
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_self_subquery AS () -> (SELECT a.size);
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_len AS () -> length(s);
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_size AS (x) -> x.size;
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_param_size AS (a) -> a.size;
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_shadowed AS (x) -> y;
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_apply AS (${CLICKHOUSE_DATABASE}_shadowed, v) -> ${CLICKHOUSE_DATABASE}_shadowed(v);

  DESCRIBE TABLE format(CSV, 'i Int32, a String ALIAS ${CLICKHOUSE_DATABASE}_self()', '1'); -- { serverError CYCLIC_ALIASES }
  DESCRIBE TABLE format(CSV, 'i Int32, a String ALIAS toString(${CLICKHOUSE_DATABASE}_self_subquery())', '1'); -- { serverError CYCLIC_ALIASES }
  DESCRIBE TABLE format(CSV, 'i Int32, a String ALIAS toString(${CLICKHOUSE_DATABASE}_size(b)), b String ALIAS toString(a.size)', '1'); -- { serverError CYCLIC_ALIASES }

  SELECT a FROM format(CSV, 'i Int32, s String, a UInt64 ALIAS ${CLICKHOUSE_DATABASE}_len()', '1,abc');
  SELECT a FROM format(CSV, 's String, a String ALIAS toString(${CLICKHOUSE_DATABASE}_param_size(s))', 'abc');
  SELECT y FROM format(CSV, 'i String, y UInt64 ALIAS ${CLICKHOUSE_DATABASE}_apply(v -> length(v), i)', 'abc');

  DROP FUNCTION ${CLICKHOUSE_DATABASE}_self;
  DROP FUNCTION ${CLICKHOUSE_DATABASE}_self_subquery;
  DROP FUNCTION ${CLICKHOUSE_DATABASE}_len;
  DROP FUNCTION ${CLICKHOUSE_DATABASE}_size;
  DROP FUNCTION ${CLICKHOUSE_DATABASE}_param_size;
  DROP FUNCTION ${CLICKHOUSE_DATABASE}_shadowed;
  DROP FUNCTION ${CLICKHOUSE_DATABASE}_apply;
"
