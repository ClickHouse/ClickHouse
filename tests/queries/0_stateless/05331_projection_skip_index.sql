-- Data skipping indices declared inside a projection definition:
--   PROJECTION p (SELECT ..., INDEX ix (expr) TYPE minmax GRANULARITY 1 ORDER BY ...)
-- The index is built over the projection's own data, so it prunes granules of the projection
-- part - both when the projection is chosen as the data source and when it is used only as an
-- index over the parent part via `_part_offset`. Aggregate projections can index their GROUP BY keys.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET optimize_use_projections = 1, optimize_use_projection_filtering = 1;
SET optimize_aggregation_in_order = 0;
SET parallel_replicas_local_plan = 1;
-- The tables below are tiny, so do not let the cost threshold skip the projection index.
SET min_table_rows_to_use_projection_index = 0;

-- { echo ON }

------------------------------------------------------------------------------
-- 1. Formatting: the INDEX declaration survives SHOW CREATE, ATTACH and JSON
------------------------------------------------------------------------------

DROP TABLE IF EXISTS t_proj_format;

CREATE TABLE t_proj_format
(
    time1 DateTime,
    message String,
    size UInt64,
    size2 UInt64,
    PROJECTION proj
    (
        SELECT _part_offset, size, size2,
            INDEX ix (size) TYPE minmax GRANULARITY 1,
            INDEX ix2 (size2) TYPE set(100) GRANULARITY 2
        WHERE size > 1
        ORDER BY message
    )
)
ENGINE = MergeTree ORDER BY time1
SETTINGS index_granularity = 1;

SHOW CREATE TABLE t_proj_format FORMAT LineAsString;

-- ATTACH parses the formatted query back from the metadata file.
DETACH TABLE t_proj_format;
ATTACH TABLE t_proj_format;
SHOW CREATE TABLE t_proj_format FORMAT LineAsString;

DROP TABLE t_proj_format;

-- A column named `index` in the projection SELECT list is an ordinary identifier, not the start of an
-- INDEX declaration. The formatter writes it unquoted, so ATTACH must be able to parse it back.
DROP TABLE IF EXISTS t_proj_index_column;

CREATE TABLE t_proj_index_column
(
    `index` UInt64,
    x UInt64,
    PROJECTION proj
    (
        SELECT index, x
        ORDER BY x
    )
)
ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 1;

SHOW CREATE TABLE t_proj_index_column FORMAT LineAsString;

DETACH TABLE t_proj_index_column;
ATTACH TABLE t_proj_index_column;
SHOW CREATE TABLE t_proj_index_column FORMAT LineAsString;

DROP TABLE t_proj_index_column;

-- As for table-level indices, the JSON AST does not keep the parentheses around a single-column
-- index expression, so `(c)` is formatted as `c`.
SELECT formatQueryFromJSON(parseQueryToJSON(q)), parseQueryToJSON(formatQueryFromJSON(parseQueryToJSON(q))) = parseQueryToJSON(q)
FROM (SELECT arrayJoin([
    'CREATE TABLE t (a UInt64, b UInt64, c UInt64, s String, PROJECTION p (SELECT _part_offset, b, c, s, INDEX ix1 (c) TYPE minmax GRANULARITY 1, INDEX ix2 lower(s) TYPE set(100) GRANULARITY 2 ORDER BY b)) ENGINE = MergeTree ORDER BY a',
    'ALTER TABLE t ADD PROJECTION p (SELECT k, sum(v), INDEX ix (k) TYPE minmax GRANULARITY 1 GROUP BY k)'
]) AS q);

-- Malformed JSON must be rejected. `skip_indices` may only hold index declarations.
SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE TABLE t (a UInt64, b UInt64, PROJECTION p (SELECT a, b, INDEX ix b TYPE minmax GRANULARITY 1 ORDER BY a)) ENGINE = MergeTree ORDER BY a$$),
    '"children":[{"type":"IndexDeclaration","name":"ix","granularity":1,"expression":{"type":"Identifier","name":"b"},"index_type":{"type":"Function","name":"minmax","no_empty_args":true}}]',
    '"children":[{"type":"Identifier","name":"b"}]')); -- { serverError BAD_ARGUMENTS }

-- The projection is not checked as a whole, so each index must reject a function without arguments in its expression.
SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE TABLE t (a UInt64, b UInt64, PROJECTION p (SELECT a, b, INDEX ix b TYPE minmax GRANULARITY 1 ORDER BY a)) ENGINE = MergeTree ORDER BY a$$),
    '"expression":{"type":"Identifier","name":"b"}',
    '"expression":{"type":"Function","name":"now"}')); -- { serverError BAD_ARGUMENTS }

------------------------------------------------------------------------------
-- 2. Validation
------------------------------------------------------------------------------

-- The index expression must only reference columns the projection stores.
CREATE TABLE t_bad (a UInt64, b UInt64, c UInt64,
    PROJECTION p (SELECT _part_offset, b, INDEX ix (c) TYPE minmax GRANULARITY 1 ORDER BY b))
ENGINE = MergeTree ORDER BY a; -- { serverError UNKNOWN_IDENTIFIER }

-- Index names must be unique within a projection.
CREATE TABLE t_bad (a UInt64, b UInt64, c UInt64, d UInt64,
    PROJECTION p (SELECT _part_offset, b, c, d,
        INDEX ix (c) TYPE minmax GRANULARITY 1,
        INDEX ix (d) TYPE minmax GRANULARITY 1 ORDER BY b))
ENGINE = MergeTree ORDER BY a; -- { serverError ILLEGAL_INDEX }

-- In an aggregate projection only GROUP BY keys can be indexed. The stored aggregate column
-- `sum(c)` holds a partial state per part, so an index over it could prune rows wrongly.
CREATE TABLE t_bad (a UInt64, b UInt64, c UInt64,
    PROJECTION p (SELECT b, sum(c), INDEX ix `sum(c)` TYPE minmax GRANULARITY 1 GROUP BY b))
ENGINE = MergeTree ORDER BY a; -- { serverError ILLEGAL_INDEX }

-- An aggregate projection is never used for queries that read a subcolumn of a GROUP BY key, so
-- an index on such a subcolumn could never prune anything.
CREATE TABLE t_bad (a UInt64, customer Tuple(id UInt64, name String),
    PROJECTION p (SELECT customer, count(), INDEX ix (customer.id) TYPE minmax GRANULARITY 1 GROUP BY customer))
ENGINE = MergeTree ORDER BY a; -- { serverError ILLEGAL_INDEX }

-- An expression over the whole key, such as `tupleElement`, depends on the key itself rather than on a
-- subcolumn, so it is accepted.
DROP TABLE IF EXISTS t_proj_tuple_key;
CREATE TABLE t_proj_tuple_key (a UInt64, customer Tuple(id UInt64, name String),
    PROJECTION p (SELECT customer, count(), INDEX ix tupleElement(customer, 'id') TYPE minmax GRANULARITY 1 GROUP BY customer))
ENGINE = MergeTree ORDER BY a;
DROP TABLE t_proj_tuple_key;

-- Text indexes are not supported in projections: merges of projection parts do not build them, so the
-- index would be lost after the first merge. Both CREATE and ALTER ADD PROJECTION reject them.
CREATE TABLE t_bad (a UInt64, s String,
    PROJECTION p (SELECT s, INDEX ix (s) TYPE text(tokenizer = 'splitByNonAlpha') ORDER BY s))
ENGINE = MergeTree ORDER BY a; -- { serverError ILLEGAL_INDEX }

DROP TABLE IF EXISTS t_proj_text;
CREATE TABLE t_proj_text (a UInt64, s String) ENGINE = MergeTree ORDER BY a;
ALTER TABLE t_proj_text ADD PROJECTION p (SELECT s, INDEX ix (s) TYPE text(tokenizer = 'splitByNonAlpha') ORDER BY s); -- { serverError ILLEGAL_INDEX }
DROP TABLE t_proj_text;

-- The projection SELECT list must not be empty.
CREATE TABLE t_bad (a UInt64, PROJECTION p (SELECT)) ENGINE = MergeTree ORDER BY a; -- { clientError SYNTAX_ERROR }

-- The projection SELECT list must contain at least one column, not only INDEX declarations,
-- even when ORDER BY would make the projection store its sorting column implicitly.
CREATE TABLE t_bad (a UInt64, PROJECTION p (SELECT INDEX ix a TYPE minmax GRANULARITY 1 ORDER BY a)) ENGINE = MergeTree ORDER BY a; -- { clientError SYNTAX_ERROR }

------------------------------------------------------------------------------
-- 3. Index on an expression, built on INSERT and on merge
------------------------------------------------------------------------------

-- The index expression is computed over the projection's block whenever a projection part is
-- written. `message` is stored in upper case, so the query finds its row only if the index holds
-- `lower(message)` rather than the raw column. The projection is sorted by `size`, so all the
-- pruning comes from the index.

DROP TABLE IF EXISTS t_proj_expr;

CREATE TABLE t_proj_expr
(
    time1 DateTime,
    message String,
    size UInt64,
    PROJECTION proj
    (
        SELECT message, size,
            INDEX ix_lower lower(message) TYPE set(100) GRANULARITY 1
        ORDER BY size
    )
)
ENGINE = MergeTree ORDER BY time1
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_proj_expr SELECT toDateTime('2026-01-01 00:00:00') + number, concat('MESSAGE_', lpad(toString(number), 6, '0')), number
FROM numbers(0, 5);
INSERT INTO t_proj_expr SELECT toDateTime('2026-01-01 00:00:00') + number, concat('MESSAGE_', lpad(toString(number), 6, '0')), number
FROM numbers(5, 5);

-- `message` and `size` both live in the projection, so the projection is read instead of the table
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT size FROM t_proj_expr WHERE lower(message) = 'message_000004')
WHERE explain LIKE '%ReadFromMergeTree%' OR match(explain, '^\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*:');

SELECT size FROM t_proj_expr WHERE lower(message) = 'message_000004';

OPTIMIZE TABLE t_proj_expr FINAL;

SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT size FROM t_proj_expr WHERE lower(message) = 'message_000004')
WHERE explain LIKE '%ReadFromMergeTree%' OR match(explain, '^\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*:');

SELECT size FROM t_proj_expr WHERE lower(message) = 'message_000004';
SELECT size FROM t_proj_expr WHERE lower(message) = 'message_000004' SETTINGS optimize_use_projections = 0;

DROP TABLE t_proj_expr;

------------------------------------------------------------------------------
-- 4. Index on an expression, built by MATERIALIZE PROJECTION
------------------------------------------------------------------------------

DROP TABLE IF EXISTS t_proj_materialize;

CREATE TABLE t_proj_materialize
(
    time1 DateTime,
    message String,
    size UInt64
)
ENGINE = MergeTree ORDER BY time1
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_proj_materialize SELECT toDateTime('2026-01-01 00:00:00') + number, concat('MESSAGE_', lpad(toString(number), 6, '0')), number
FROM numbers(10);

ALTER TABLE t_proj_materialize ADD PROJECTION proj
(
    SELECT message, size,
        INDEX ix_lower lower(message) TYPE set(100) GRANULARITY 1
    ORDER BY size
);
ALTER TABLE t_proj_materialize MATERIALIZE PROJECTION proj SETTINGS mutations_sync = 2;

SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT size FROM t_proj_materialize WHERE lower(message) = 'message_000004')
WHERE explain LIKE '%ReadFromMergeTree%' OR match(explain, '^\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*:');

SELECT size FROM t_proj_materialize WHERE lower(message) = 'message_000004';
SELECT size FROM t_proj_materialize WHERE lower(message) = 'message_000004' SETTINGS optimize_use_projections = 0;

DROP TABLE t_proj_materialize;

------------------------------------------------------------------------------
-- 5. Normal projection as an index over the parent part
------------------------------------------------------------------------------

-- `payload` is not in the projection, so the projection cannot answer the query; it is used
-- only to filter the parent's mark ranges through `_part_offset`. The projection is sorted by
-- `message`, so its primary key cannot prune on `size2` (`Condition: true`) and all the
-- filtering comes from `ix_size2`.

DROP TABLE IF EXISTS t_proj_as_index;

CREATE TABLE t_proj_as_index
(
    time1 DateTime,
    message String,
    size UInt64,
    size2 UInt64,
    payload String,
    PROJECTION proj
    (
        SELECT _part_offset, message, size2,
            INDEX ix_size2 (size2) TYPE minmax GRANULARITY 1
        ORDER BY message
    )
)
ENGINE = MergeTree ORDER BY time1
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_proj_as_index SELECT toDateTime('2026-01-01 00:00:00') + number,
    concat('message_', lpad(toString(number), 6, '0')), number, number * 2, 'p'
FROM numbers(10);

SELECT trimLeft(explain)
FROM (EXPLAIN projections = 1 SELECT payload FROM t_proj_as_index WHERE size2 = 8)
WHERE explain LIKE '%ReadFromMergeTree%' OR match(explain, '^\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*:');

SELECT payload, size FROM t_proj_as_index WHERE size2 = 8;
SELECT payload, size FROM t_proj_as_index WHERE size2 = 8
SETTINGS optimize_use_projections = 0, optimize_use_projection_filtering = 0;

DROP TABLE t_proj_as_index;

------------------------------------------------------------------------------
-- 6. Aggregate projection: an index on a GROUP BY key
------------------------------------------------------------------------------

-- The projection is sorted by (`k1`, `k2`), and every row has its own `k1`, so the primary key
-- barely prunes on `k2` and the pruning comes from the index.

DROP TABLE IF EXISTS t_proj_agg;

CREATE TABLE t_proj_agg
(
    time1 DateTime,
    k1 UInt64,
    k2 UInt64,
    value UInt64,
    PROJECTION proj
    (
        SELECT k1, k2, sum(value),
            INDEX ix (k2) TYPE minmax GRANULARITY 1
        GROUP BY k1, k2
    )
)
ENGINE = MergeTree ORDER BY time1
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_proj_agg SELECT toDateTime('2026-01-01 00:00:00') + number, number, number % 5, number
FROM numbers(10);

SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT k2, sum(value) FROM t_proj_agg WHERE k2 = 2 GROUP BY k2)
WHERE explain LIKE '%ReadFromMergeTree%' OR match(explain, '^\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*:');

SELECT k2, sum(value) FROM t_proj_agg WHERE k2 = 2 GROUP BY k2;
SELECT k2, sum(value) FROM t_proj_agg WHERE k2 = 2 GROUP BY k2 SETTINGS optimize_use_projections = 0;

DROP TABLE t_proj_agg;

------------------------------------------------------------------------------
-- 7. Index types do not depend on the settings of the `ALTER` session
------------------------------------------------------------------------------

-- `enable_extended_results_for_datetime_functions` changes the type of `toStartOfHour` over a `DateTime64`
-- column from `DateTime` to `DateTime64`. Inserts and merges compute the index with the server settings, so
-- the type recorded for the index must not come from the session that runs the `ALTER`. First, the index
-- is added with `ADD PROJECTION`.

DROP TABLE IF EXISTS t_proj_alter_settings;

CREATE TABLE t_proj_alter_settings (k UInt64, ts DateTime64(3)) ENGINE = MergeTree ORDER BY k;

ALTER TABLE t_proj_alter_settings ADD PROJECTION proj
    (SELECT k, ts, INDEX ix toStartOfHour(ts) TYPE set(100) GRANULARITY 1 ORDER BY k)
    SETTINGS enable_extended_results_for_datetime_functions = 1;

INSERT INTO t_proj_alter_settings SELECT number, toDateTime64('2026-01-01 00:00:00', 3) FROM numbers(1);

DROP TABLE t_proj_alter_settings;

-- Second, an `ALTER` that does not touch the projection analyzes all projections again. It must not change
-- the type the index already has.

DROP TABLE IF EXISTS t_proj_reanalyze;

CREATE TABLE t_proj_reanalyze
(
    k UInt64,
    ts DateTime64(3),
    PROJECTION proj (SELECT k, ts, INDEX ix toStartOfHour(ts) TYPE set(100) GRANULARITY 1 ORDER BY k)
)
ENGINE = MergeTree ORDER BY k;

INSERT INTO t_proj_reanalyze SELECT number, toDateTime64('2026-01-01 00:00:00', 3) FROM numbers(1);

ALTER TABLE t_proj_reanalyze ADD COLUMN extra UInt8 SETTINGS enable_extended_results_for_datetime_functions = 1;
OPTIMIZE TABLE t_proj_reanalyze FINAL;

DROP TABLE t_proj_reanalyze;
