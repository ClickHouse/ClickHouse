#!/usr/bin/env bash
# The null map of a Nullable field of a Variant/Dynamic/JSON element must equal isNull() of that
# field: 1 wherever the element is absent, the field's own bit elsewhere. allow_nullable_tuple_in_
# extracted_subcolumns is read from the global context, so it has to be passed on the command line -
# a session level SET does not reach the extraction - and both of its values are exercised below.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# With the setting off the extracted tuple is not Nullable, so the null map is the only place that can
# say the element is absent. Part thresholds are pinned so that the compact and the wide reader, which
# resolve the subcolumn through different code, are each covered on every run.
${CLICKHOUSE_LOCAL} --allow_nullable_tuple_in_extracted_subcolumns=0 --enable_variant_type=1 --query "
CREATE TABLE t_dyn (id UInt64, value Dynamic) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_dyn VALUES (1, CAST(tuple(CAST(1, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
INSERT INTO t_dyn VALUES (2, CAST(tuple(CAST(NULL, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
INSERT INTO t_dyn VALUES (3, 'not a tuple');

SELECT 'type', toTypeName(value.\`Tuple(a Nullable(UInt32), b String)\`.a.null) FROM t_dyn LIMIT 1;

-- Three unmerged parts: every part holds rows of one alternative only.
SELECT 'unmerged', id, value.\`Tuple(a Nullable(UInt32), b String)\`.a AS v,
       v IS NULL AS is_null, value.\`Tuple(a Nullable(UInt32), b String)\`.a.null AS null_sub
FROM t_dyn ORDER BY id;

OPTIMIZE TABLE t_dyn FINAL;

-- One merged part: row 3's discriminator is not the tuple's, which is the case that was wrong.
SELECT 'merged', id, value.\`Tuple(a Nullable(UInt32), b String)\`.a AS v,
       v IS NULL AS is_null, value.\`Tuple(a Nullable(UInt32), b String)\`.a.null AS null_sub
FROM t_dyn ORDER BY id;

-- optimize_functions_to_subcolumns rewrites isNull(x) to x.null, so the two must agree.
SELECT 'count rewritten', count() FROM t_dyn
WHERE value.\`Tuple(a Nullable(UInt32), b String)\`.a IS NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'count direct', count() FROM t_dyn
WHERE value.\`Tuple(a Nullable(UInt32), b String)\`.a IS NULL SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'count not null rewritten', count() FROM t_dyn
WHERE value.\`Tuple(a Nullable(UInt32), b String)\`.a IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'count not null direct', count() FROM t_dyn
WHERE value.\`Tuple(a Nullable(UInt32), b String)\`.a IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 0;

-- The counts above only agree while the rewrite still happens for this nested subcolumn, so pin that
-- it does. The second line is the control: without the rewrite the null map must not be in the tree.
SELECT 'rewrite fires', countIf(explain LIKE '%.a.null%') > 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM t_dyn WHERE value.\`Tuple(a Nullable(UInt32), b String)\`.a IS NULL)
SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'rewrite off', countIf(explain LIKE '%.a.null%') > 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM t_dyn WHERE value.\`Tuple(a Nullable(UInt32), b String)\`.a IS NULL)
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'prewhere', id FROM t_dyn
PREWHERE value.\`Tuple(a Nullable(UInt32), b String)\`.a.null ORDER BY id;

-- Reads the null map and the value of the same column in one statement, so both serializations are
-- built while one serialization pool is alive and the pooling key has to tell them apart. The rewrite
-- is off so that the two sides really are a value read and a null map read, not the same subcolumn.
SELECT 'pooling mismatches', countIf(is_null != null_sub) FROM (
    SELECT value.\`Tuple(a Nullable(UInt32), b String)\`.a IS NULL AS is_null,
           value.\`Tuple(a Nullable(UInt32), b String)\`.a.null AS null_sub FROM t_dyn)
SETTINGS optimize_functions_to_subcolumns = 0;

-- A wide part uses a different reader.
CREATE TABLE t_dyn_wide (id UInt64, value Dynamic) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_dyn_wide SELECT * FROM t_dyn;
SELECT 'wide', id, value.\`Tuple(a Nullable(UInt32), b String)\`.a.null FROM t_dyn_wide ORDER BY id;

-- A plain Variant column goes through the generic subcolumn creator instead of DataTypeDynamic.
CREATE TABLE t_var (id UInt64, value Variant(Tuple(a Nullable(UInt32), b String), String)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_var VALUES (1, CAST(tuple(CAST(1, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
INSERT INTO t_var VALUES (2, CAST(tuple(CAST(NULL, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
INSERT INTO t_var VALUES (3, 'not a tuple');
OPTIMIZE TABLE t_var FINAL;
SELECT 'variant', id, value.\`Tuple(a Nullable(UInt32), b String)\`.a.null FROM t_var ORDER BY id;

-- The in-memory path builds the subcolumn with IColumn::expand instead of reading streams.
CREATE TABLE t_mem (id UInt64, value Dynamic) ENGINE = Memory;
INSERT INTO t_mem SELECT * FROM t_dyn;
SELECT 'memory', id, value.\`Tuple(a Nullable(UInt32), b String)\`.a.null FROM t_mem ORDER BY id;

-- One granule per row, so the rows the element is absent from are read as ranges of their own rather
-- than mixed in with rows that have it.
CREATE TABLE t_gran (id UInt64, value Dynamic) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 1;
INSERT INTO t_gran VALUES (1, CAST(tuple(CAST(1, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
INSERT INTO t_gran VALUES (2, 'not a tuple'), (3, 'still not a tuple');
INSERT INTO t_gran VALUES (4, CAST(tuple(CAST(NULL, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
OPTIMIZE TABLE t_gran FINAL;
SELECT 'granule absent', id, value.\`Tuple(a Nullable(UInt32), b String)\`.a.null FROM t_gran ORDER BY id;

-- A part written before the alternative existed has no stream for the element, so the read of it
-- yields nothing at all rather than a run of foreign discriminators.
CREATE TABLE t_alter (id UInt64, value Variant(String)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_alter VALUES (1, 'not a tuple');
ALTER TABLE t_alter MODIFY COLUMN value Variant(Tuple(a Nullable(UInt32), b String), String);
INSERT INTO t_alter VALUES (2, CAST(tuple(CAST(7, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
SELECT 'missing stream', id, value.\`Tuple(a Nullable(UInt32), b String)\`.a.null FROM t_alter ORDER BY id;

-- A pure Variant column added by ALTER: the part predates the column, so the read finds no
-- discriminators stream at all rather than a run of foreign discriminators.
CREATE TABLE t_var_add (id UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_var_add VALUES (1);
ALTER TABLE t_var_add ADD COLUMN value Variant(Tuple(a Nullable(UInt32), b String), String);
INSERT INTO t_var_add VALUES (2, CAST(tuple(CAST(7, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
SELECT 'variant added column', id, value.\`Tuple(a Nullable(UInt32), b String)\`.a.null FROM t_var_add ORDER BY id;

-- The wide twin of the case above: a wide part keeps every column in its own file, so the reader
-- reaches the absent element by a different route than in a compact part.
CREATE TABLE t_var_add_wide (id UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_var_add_wide VALUES (1);
ALTER TABLE t_var_add_wide ADD COLUMN value Variant(Tuple(a Nullable(UInt32), b String), String);
INSERT INTO t_var_add_wide VALUES (2, CAST(tuple(CAST(7, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
SELECT 'variant added column wide', id, value.\`Tuple(a Nullable(UInt32), b String)\`.a.null FROM t_var_add_wide ORDER BY id;

-- A part written before the column existed carries no Dynamic structure at all, so the read has no
-- state to work from.
CREATE TABLE t_add (id UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_add VALUES (1);
ALTER TABLE t_add ADD COLUMN value Dynamic;
INSERT INTO t_add VALUES (2, CAST(tuple(CAST(7, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
SELECT 'added column', id, value.\`Tuple(a Nullable(UInt32), b String)\`.a.null FROM t_add ORDER BY id;

-- Control: a part with no rows of the alternative, read together with a part that has them. The
-- element is not a variant of that part at all, which a different branch already answered with 1.
CREATE TABLE t_absent (id UInt64, value Dynamic) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_absent VALUES (1, 'not a tuple'), (2, 'also not a tuple');
INSERT INTO t_absent VALUES (3, CAST(tuple(CAST(5, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
SELECT 'control absent part', id, value.\`Tuple(a Nullable(UInt32), b String)\`.a.null FROM t_absent ORDER BY id;

-- Control: the alternative lands in the shared variant, which is decoded per row into a default
-- tuple whose element is already NULL, so this branch was correct before the fix as well.
CREATE TABLE t_shared (id UInt64, value Dynamic(max_types = 0)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_shared VALUES (1, CAST(tuple(CAST(1, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
INSERT INTO t_shared VALUES (2, CAST(tuple(CAST(NULL, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
INSERT INTO t_shared VALUES (3, 'not a tuple');
OPTIMIZE TABLE t_shared FINAL;
SELECT 'control shared variant', id, value.\`Tuple(a Nullable(UInt32), b String)\`.a.null FROM t_shared ORDER BY id;

-- The in-memory twin of the case above: with the value in the shared variant, the extraction yields a
-- column of only the matching rows, which the subcolumn creator expands rather than a stream read.
CREATE TABLE t_shared_mem (id UInt64, value Dynamic(max_types = 0)) ENGINE = Memory;
INSERT INTO t_shared_mem VALUES (1, CAST(tuple(CAST(1, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
INSERT INTO t_shared_mem VALUES (2, CAST(tuple(CAST(NULL, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
INSERT INTO t_shared_mem VALUES (3, 'not a tuple');
SELECT 'memory shared variant', id, value.\`Tuple(a Nullable(UInt32), b String)\`.a AS v,
       v IS NULL AS is_null, value.\`Tuple(a Nullable(UInt32), b String)\`.a.null AS null_sub
FROM t_shared_mem ORDER BY id;

-- No row holds the requested type and nothing is in the shared variant, so the subcolumn is built from
-- nothing at all rather than expanded from a partial one.
CREATE TABLE t_absent_mem (id UInt64, value Dynamic) ENGINE = Memory;
INSERT INTO t_absent_mem VALUES (1, 'not a tuple'), (2, CAST(5, 'UInt32'));
SELECT 'memory absent type', id, value.\`Tuple(a Nullable(UInt32), b String)\`.a AS v,
       v IS NULL AS is_null, value.\`Tuple(a Nullable(UInt32), b String)\`.a.null AS null_sub
FROM t_absent_mem ORDER BY id;

-- A JSON typed path whose type is a Variant, and a JSON dynamic path with a type hint.
CREATE TABLE t_json (id UInt64, value JSON(t Variant(Tuple(a Nullable(UInt32), b String), String)))
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_json VALUES (1, '{\"t\": {\"a\": 1, \"b\": \"s\"}}');
INSERT INTO t_json VALUES (2, '{\"t\": {\"a\": null, \"b\": \"s\"}}');
INSERT INTO t_json VALUES (3, '{\"t\": \"not a tuple\"}');
OPTIMIZE TABLE t_json FINAL;
SELECT 'json typed path', id, value.t.\`Tuple(a Nullable(UInt32), b String)\`.a.null FROM t_json ORDER BY id;

-- A JSON dynamic path reached with a type hint no row of the column holds: the requested type is
-- absent altogether, which is served by a separate branch of DataTypeDynamic::getSubcolumnData.
CREATE TABLE t_json_dyn (id UInt64, value JSON) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_json_dyn VALUES (1, '{\"k\": {\"a\": 1, \"b\": \"s\"}}');
INSERT INTO t_json_dyn VALUES (2, '{\"k\": \"not a tuple\"}');
OPTIMIZE TABLE t_json_dyn FINAL;
SELECT 'json dynamic path absent type', id, value.k.:\`Tuple(a Nullable(UInt32), b String)\`.a.null FROM t_json_dyn ORDER BY id;

-- The rest of the name resolved dynamically below the element, so the selected null map is the end of
-- the JSON path rather than the element the outer creator matched.
CREATE TABLE t_descendant (id UInt64, value Variant(Tuple(a JSON), String)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_descendant VALUES (1, CAST(tuple(CAST('{\"k\": 1}', 'JSON')), 'Tuple(a JSON)'));
INSERT INTO t_descendant VALUES (2, 'not a tuple');
OPTIMIZE TABLE t_descendant FINAL;
SELECT 'dynamic descendant', id, value.\`Tuple(a JSON)\`.a.k.:Int64.null FROM t_descendant ORDER BY id;

-- Nested shapes below the element: a Variant, and one more Tuple level.
CREATE TABLE t_nested (id UInt64, value Dynamic) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_nested VALUES (1, CAST(tuple(CAST(CAST(1, 'UInt32'), 'Variant(String, UInt32)')), 'Tuple(a Variant(String, UInt32))'));
INSERT INTO t_nested VALUES (2, CAST(tuple(tuple(CAST(9, 'Nullable(UInt32)'))), 'Tuple(a Tuple(x Nullable(UInt32)))'));
INSERT INTO t_nested VALUES (3, 'not a tuple');
OPTIMIZE TABLE t_nested FINAL;
SELECT 'nested variant', id, value.\`Tuple(a Variant(String, UInt32))\`.a.UInt32.null FROM t_nested ORDER BY id;
SELECT 'nested tuple', id, value.\`Tuple(a Tuple(x Nullable(UInt32)))\`.a.x.null FROM t_nested ORDER BY id;

-- Controls that must not change. A genuine UInt8 element keeps the default 0 on an absent row, and an
-- Array or Map wrapper turns the null map into Array(UInt8), whose absent value is the default [].
CREATE TABLE t_controls (id UInt64, value Dynamic) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_controls VALUES (1, CAST(tuple(CAST(1, 'UInt8'), 's'), 'Tuple(a UInt8, b String)'));
INSERT INTO t_controls VALUES (2, CAST(tuple([CAST(NULL, 'Nullable(UInt32)')]), 'Tuple(a Array(Nullable(UInt32)))'));
INSERT INTO t_controls VALUES (3, CAST(tuple(map('k', CAST(NULL, 'Nullable(UInt32)'))), 'Tuple(a Map(String, Nullable(UInt32)))'));
INSERT INTO t_controls VALUES (4, 'not a tuple');
INSERT INTO t_controls VALUES (5, CAST(5, 'UInt32'));
OPTIMIZE TABLE t_controls FINAL;
SELECT 'control uint8', id, toTypeName(value.\`Tuple(a UInt8, b String)\`.a),
       value.\`Tuple(a UInt8, b String)\`.a FROM t_controls ORDER BY id;
SELECT 'control array', id, toTypeName(value.\`Tuple(a Array(Nullable(UInt32)))\`.a.null),
       value.\`Tuple(a Array(Nullable(UInt32)))\`.a.null FROM t_controls ORDER BY id;
SELECT 'control map', id, toTypeName(value.\`Tuple(a Map(String, Nullable(UInt32)))\`.a.values.null),
       value.\`Tuple(a Map(String, Nullable(UInt32)))\`.a.values.null FROM t_controls ORDER BY id;

-- The null map OF an element, synthesised from the discriminators alone by a different serialization.
-- It is the contrast the nested null map above now matches.
SELECT 'element null map', id, value.UInt32.null FROM t_controls ORDER BY id;
"

# With the setting on the extracted tuple is itself Nullable, so absence is already expressed as NULL
# and none of the above applies: this invocation must print exactly what it printed before the fix.
${CLICKHOUSE_LOCAL} --allow_nullable_tuple_in_extracted_subcolumns=1 --enable_variant_type=1 --query "
CREATE TABLE t_dyn (id UInt64, value Dynamic) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_dyn VALUES (1, CAST(tuple(CAST(1, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
INSERT INTO t_dyn VALUES (2, CAST(tuple(CAST(NULL, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
INSERT INTO t_dyn VALUES (3, 'not a tuple');
OPTIMIZE TABLE t_dyn FINAL;
SELECT 'setting on type', toTypeName(value.\`Tuple(a Nullable(UInt32), b String)\`.a.null) FROM t_dyn LIMIT 1;
SELECT 'setting on', id, value.\`Tuple(a Nullable(UInt32), b String)\`.a.null FROM t_dyn ORDER BY id;

CREATE TABLE t_var (id UInt64, value Variant(Tuple(a Nullable(UInt32), b String), String)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_var VALUES (1, CAST(tuple(CAST(1, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
INSERT INTO t_var VALUES (2, CAST(tuple(CAST(NULL, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
INSERT INTO t_var VALUES (3, 'not a tuple');
OPTIMIZE TABLE t_var FINAL;
SELECT 'setting on variant', id, value.\`Tuple(a Nullable(UInt32), b String)\`.a.null FROM t_var ORDER BY id;

CREATE TABLE t_shared (id UInt64, value Dynamic(max_types = 0)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_shared VALUES (1, CAST(tuple(CAST(1, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
INSERT INTO t_shared VALUES (2, 'not a tuple');
OPTIMIZE TABLE t_shared FINAL;
SELECT 'setting on shared', id, value.\`Tuple(a Nullable(UInt32), b String)\`.a.null FROM t_shared ORDER BY id;
"
