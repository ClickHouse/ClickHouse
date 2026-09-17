-- The reject for an index over a mistyped ALIAS column covers every index type whose condition
-- matches a query predicate to the indexed expression by its exact column name: `text`,
-- `tokenbf_v1`, `ngrambf_v1`, `sparse_grams` and `bloom_filter`. For all of them the `_CAST` the
-- column is read through can never equal the indexed expression, so the index is dead.
-- `minmax` (the cast is traversed and the index stays usable when it is monotonic) and `set`
-- (matches the expression DAG through the cast) remain allowed.
-- The `text` type itself is covered by 05076_text_index_on_mistyped_alias_column.

DROP TABLE IF EXISTS t_mistyped;

-- Each name-matched index type is rejected over an ALIAS declared as a type its expression
-- does not produce (`lower(event)` produces `String`, the column is declared `FixedString(3)`).
CREATE TABLE t_mistyped (event String, tok FixedString(3) ALIAS lower(event), INDEX i tok TYPE tokenbf_v1(256, 2, 0))
    ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_mistyped (event String, tok FixedString(3) ALIAS lower(event), INDEX i tok TYPE ngrambf_v1(3, 256, 2, 0))
    ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_mistyped (event String, tok FixedString(3) ALIAS lower(event), INDEX i tok TYPE sparse_grams(3, 20, 256, 2, 0))
    ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_mistyped (event String, tok FixedString(3) ALIAS lower(event), INDEX i tok TYPE bloom_filter(0.01))
    ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- Introducing the same mismatch through ALTER is rejected the same way.
CREATE TABLE t_mistyped (event String, tok FixedString(3) ALIAS lower(event)) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_mistyped ADD INDEX i tok TYPE bloom_filter(0.01); -- { serverError BAD_ARGUMENTS }
DROP TABLE t_mistyped;

-- The same declarations with the ALIAS declared as the type its expression produces are accepted.
CREATE TABLE t_welltyped
(
    event String,
    tok String ALIAS lower(event),
    INDEX i_tokenbf tok TYPE tokenbf_v1(256, 2, 0),
    INDEX i_ngrambf tok TYPE ngrambf_v1(3, 256, 2, 0),
    INDEX i_sparse tok TYPE sparse_grams(3, 20, 256, 2, 0),
    INDEX i_bloom tok TYPE bloom_filter(0.01)
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_welltyped VALUES ('AbC'), ('dEf');
SELECT count() FROM t_welltyped WHERE hasToken(tok, 'abc');
DROP TABLE t_welltyped;

-- Negative controls: over the very same mistyped ALIAS, an index type whose condition can still
-- match through the cast is accepted and remains usable.
CREATE TABLE t_still_usable
(
    event String,
    tok FixedString(3) ALIAS lower(event),
    INDEX i_minmax tok TYPE minmax,
    INDEX i_set tok TYPE set(100)
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_still_usable VALUES ('AbC'), ('dEf');
SELECT count() FROM t_still_usable WHERE tok = toFixedString('abc', 3);
DROP TABLE t_still_usable;

-- A declared type that differs from the expression only by a `LowCardinality` wrapper is not a
-- mistype: the index conditions strip `LowCardinality` before matching, so the index still works.
-- `k * 2` produces `UInt64`; `k` is `LowCardinality(UInt64)`, so declaring `lc` that way matches.
CREATE TABLE t_low_cardinality
(
    k UInt64,
    lc LowCardinality(UInt64) ALIAS k,
    doubled UInt64 ALIAS lc * 2,
    INDEX i_bloom doubled TYPE bloom_filter GRANULARITY 1
) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_suspicious_low_cardinality_types = 1;
INSERT INTO t_low_cardinality VALUES (3), (4);
SELECT count() FROM t_low_cardinality WHERE doubled = 6;
DROP TABLE t_low_cardinality;
