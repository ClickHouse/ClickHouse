#!/usr/bin/env bash
# Reading a whole `Tuple` subcolumn of a `Dynamic` or `Variant` column out of a MergeTree part, while
# extracted subcolumns are allowed to be `Nullable`.
# `allow_nullable_tuple_in_extracted_subcolumns` is read from the global context, so it has to be passed
# on the command line: a session level SET does not reach the extraction (see
# 03918_allow_nullable_tuple_in_extracted_subcolumns_not_changeable).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --allow_nullable_tuple_in_extracted_subcolumns=1 --query "
-- The extracted whole-tuple subcolumn is Nullable only while the setting is on.
SELECT toTypeName(value.\`Tuple(a UInt32, b String)\`)
FROM (SELECT CAST(tuple(1, 's'), 'Tuple(a UInt32, b String)')::Dynamic AS value);

CREATE TABLE t_dyn_compact (id UInt64, value Dynamic) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_dyn_compact VALUES (1, CAST(tuple(1, 's'), 'Tuple(a UInt32, b String)')), (2, 42::UInt64);
SELECT id, value.\`Tuple(a UInt32, b String)\` FROM t_dyn_compact ORDER BY id;
SELECT id, value.\`Tuple(a UInt32, b String)\`.a FROM t_dyn_compact ORDER BY id;
SELECT id, value.\`Tuple(a UInt32, b String)\`.null FROM t_dyn_compact ORDER BY id;

CREATE TABLE t_dyn_wide (id UInt64, value Dynamic) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_dyn_wide VALUES (1, CAST(tuple(1, 's'), 'Tuple(a UInt32, b String)')), (2, 42::UInt64);
SELECT id, value.\`Tuple(a UInt32, b String)\` FROM t_dyn_wide ORDER BY id;

CREATE TABLE t_var (id UInt64, value Variant(Tuple(a UInt32, b String), UInt64))
    ENGINE = MergeTree ORDER BY id;
INSERT INTO t_var VALUES (1, CAST(tuple(1, 's'), 'Tuple(a UInt32, b String)')), (2, 42::UInt64);
SELECT id, value.\`Tuple(a UInt32, b String)\` FROM t_var ORDER BY id;

CREATE TABLE t_dyn_nullable_elem (id UInt64, value Dynamic) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dyn_nullable_elem VALUES
    (1, CAST(tuple(CAST(NULL, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)')), (2, 42::UInt64);
SELECT id, value.\`Tuple(a Nullable(UInt32), b String)\` FROM t_dyn_nullable_elem ORDER BY id;

-- max_types = 0 leaves no typed variant, so the tuple can only come back from the shared variant.
CREATE TABLE t_dyn_shared (id UInt64, value Dynamic(max_types = 0)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dyn_shared VALUES (1, CAST(tuple(1, 's'), 'Tuple(a UInt32, b String)')), (2, 42::UInt64);
SELECT id, value.\`Tuple(a UInt32, b String)\` FROM t_dyn_shared ORDER BY id;
"
