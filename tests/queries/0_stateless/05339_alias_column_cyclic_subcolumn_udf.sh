#!/usr/bin/env bash

# An ALIAS column that reads itself through a subcolumn hidden in a SQL user-defined function
# of a table function structure fails with CYCLIC_ALIASES.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Without prefer_column_name_to_alias the default expression validation already fails with `Recursive lambda`.
$CLICKHOUSE_CLIENT --prefer_column_name_to_alias 1 -q "
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_self AS () -> a.size;
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_self_subquery AS () -> (SELECT a.size);
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_size AS (x) -> x.size;
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_shadowed AS (x) -> y;
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_apply AS (${CLICKHOUSE_DATABASE}_shadowed, v) -> ${CLICKHOUSE_DATABASE}_shadowed(v);

  SELECT a FROM format(CSV, 'i Int32, a String ALIAS ${CLICKHOUSE_DATABASE}_self()', '1'); -- { serverError CYCLIC_ALIASES }
  SELECT a FROM format(CSV, 'i Int32, a String ALIAS toString(${CLICKHOUSE_DATABASE}_self_subquery())', '1'); -- { serverError CYCLIC_ALIASES }
  SELECT a FROM format(CSV, 'i Int32, a String ALIAS toString(${CLICKHOUSE_DATABASE}_size(b)), b String ALIAS toString(a.size)', '1'); -- { serverError CYCLIC_ALIASES }

  -- Not a cycle: the parameter is called with the composition, not with the function of the same name.
  SELECT y FROM format(CSV, 'i Int64, y String ALIAS ${CLICKHOUSE_DATABASE}_apply(negate | toString, i)', '7');

  DROP FUNCTION ${CLICKHOUSE_DATABASE}_self;
  DROP FUNCTION ${CLICKHOUSE_DATABASE}_self_subquery;
  DROP FUNCTION ${CLICKHOUSE_DATABASE}_size;
  DROP FUNCTION ${CLICKHOUSE_DATABASE}_shadowed;
  DROP FUNCTION ${CLICKHOUSE_DATABASE}_apply;
"
