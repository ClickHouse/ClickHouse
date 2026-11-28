#!/usr/bin/env bash
# allow_nullable_tuple_in_extracted_subcolumns is read from the global context, so it has to be
# passed on the command line - a session level SET does not reach the extraction.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# With the setting on, the whole tuple alternative is itself exposed as Nullable, while the requested
# tuple element keeps its own Nullable. Both must be readable.
${CLICKHOUSE_LOCAL} --allow_nullable_tuple_in_extracted_subcolumns=1 --enable_variant_type=1 --query "
CREATE TABLE t_var (id UInt64, value Variant(Tuple(a Nullable(UInt32), b String), String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_var VALUES (1, CAST(tuple(CAST(1, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
INSERT INTO t_var VALUES (2, CAST(tuple(CAST(NULL, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
INSERT INTO t_var VALUES (3, 'not a tuple');
SELECT toTypeName(value.\`Tuple(a Nullable(UInt32), b String)\`) FROM t_var LIMIT 1;
SELECT toTypeName(value.\`Tuple(a Nullable(UInt32), b String)\`.a) FROM t_var LIMIT 1;
SELECT id, value.\`Tuple(a Nullable(UInt32), b String)\`.a FROM t_var ORDER BY id;

CREATE TABLE t_dyn (id UInt64, value Dynamic) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dyn VALUES (1, CAST(tuple(CAST(1, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
INSERT INTO t_dyn VALUES (2, CAST(5, 'UInt32'));
SELECT id, value.\`Tuple(a Nullable(UInt32), b String)\`.a FROM t_dyn ORDER BY id;
SELECT id, value.UInt32 FROM t_dyn ORDER BY id;
"
