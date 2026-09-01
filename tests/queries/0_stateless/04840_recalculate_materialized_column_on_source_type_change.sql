-- A MATERIALIZED column stores values computed from other columns. When ALTER MODIFY COLUMN changes the
-- type of such a source column and the conversion changes values, the stored values stop matching the
-- expression, so the mutation must recalculate them (as ALTER UPDATE of the same column already does).

-- Case 1: the whole source column is used in the expression.
DROP TABLE IF EXISTS t_mat_source_type;

CREATE TABLE t_mat_source_type (x Int64, m Int64 MATERIALIZED x) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_mat_source_type VALUES (5000000000);

ALTER TABLE t_mat_source_type MODIFY COLUMN x Int32 SETTINGS mutations_sync = 2;

SELECT x, m, m = x FROM t_mat_source_type;

DROP TABLE t_mat_source_type;

-- Case 2: a subcolumn of the source column is used in the expression.
DROP TABLE IF EXISTS t_mat_source_subcolumn;

CREATE TABLE t_mat_source_subcolumn (t Tuple(a Int64, b String), m Int64 MATERIALIZED t.a)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_mat_source_subcolumn VALUES ((5000000000, 'x'));

ALTER TABLE t_mat_source_subcolumn MODIFY COLUMN t Tuple(a Int32, b String) SETTINGS mutations_sync = 2;

SELECT t.a, m, m = t.a FROM t_mat_source_subcolumn;

DROP TABLE t_mat_source_subcolumn;

-- Case 3: a skip index on the recalculated MATERIALIZED column must be rebuilt as well, otherwise it
-- keeps describing the old values.
DROP TABLE IF EXISTS t_mat_source_index;

CREATE TABLE t_mat_source_index (x Int64, m Int64 MATERIALIZED x, INDEX idx m TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4;

INSERT INTO t_mat_source_index SELECT 5000000000 + number FROM numbers(16);

ALTER TABLE t_mat_source_index MODIFY COLUMN x Int32 SETTINGS mutations_sync = 2;

SELECT countIf(m = x) FROM t_mat_source_index;
SELECT count() FROM t_mat_source_index WHERE m > 705032704 SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_mat_source_index WHERE m > 705032704 SETTINGS use_skip_indexes = 0;

DROP TABLE t_mat_source_index;

-- Case 4: a MATERIALIZED column in the sorting key cannot be recalculated in place, so a conversion that
-- can change its values must be rejected instead of leaving the key stale.
DROP TABLE IF EXISTS t_mat_source_sorting_key;

CREATE TABLE t_mat_source_sorting_key (x Int64, m Int64 MATERIALIZED x) ENGINE = MergeTree ORDER BY m
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_mat_source_sorting_key SELECT 2147483640 + number FROM numbers(10);

ALTER TABLE t_mat_source_sorting_key MODIFY COLUMN x Int32; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT countIf(m = x) FROM t_mat_source_sorting_key;

DROP TABLE t_mat_source_sorting_key;

-- Case 5: the same for a MATERIALIZED column in the partition key.
DROP TABLE IF EXISTS t_mat_source_partition_key;

CREATE TABLE t_mat_source_partition_key (id UInt32, x Int64, neg UInt8 MATERIALIZED x < 0)
ENGINE = MergeTree PARTITION BY neg ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_mat_source_partition_key SELECT number, 2147483640 + number FROM numbers(10);

ALTER TABLE t_mat_source_partition_key MODIFY COLUMN x Int32; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT countIf(neg = (x < 0)) FROM t_mat_source_partition_key;

DROP TABLE t_mat_source_partition_key;

-- Case 6: a conversion that preserves values is still allowed for a key MATERIALIZED column, and its
-- data stays correct without being rewritten.
DROP TABLE IF EXISTS t_mat_source_safe_key;

CREATE TABLE t_mat_source_safe_key (x LowCardinality(String), m String MATERIALIZED x)
ENGINE = MergeTree ORDER BY m SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_mat_source_safe_key SELECT toString(number) FROM numbers(10);

ALTER TABLE t_mat_source_safe_key MODIFY COLUMN x String SETTINGS mutations_sync = 2;

SELECT countIf(m = x) FROM t_mat_source_safe_key;

DROP TABLE t_mat_source_safe_key;

-- Case 7: a projection over the recalculated MATERIALIZED column has to be rebuilt as well, otherwise its
-- own copy of the column and its primary index keep describing the old values.
DROP TABLE IF EXISTS t_mat_source_projection;

CREATE TABLE t_mat_source_projection (id UInt32, x Int64, m Int64 MATERIALIZED x, PROJECTION p (SELECT * ORDER BY m))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4;

INSERT INTO t_mat_source_projection SELECT number, 5000000000 + number FROM numbers(16);

ALTER TABLE t_mat_source_projection MODIFY COLUMN x Int32 SETTINGS mutations_sync = 2;

SELECT countIf(m = x) FROM t_mat_source_projection;
SELECT count() FROM t_mat_source_projection WHERE m > 705032710 SETTINGS optimize_use_projections = 1;
SELECT count() FROM t_mat_source_projection WHERE m > 705032710 SETTINGS optimize_use_projections = 0;

DROP TABLE t_mat_source_projection;

-- Case 8: a TTL expression over the recalculated MATERIALIZED column has to be recalculated too. The
-- conversion moves the TTL value into the past, so the rows must expire.
DROP TABLE IF EXISTS t_mat_source_ttl;

CREATE TABLE t_mat_source_ttl (x UInt64, m DateTime MATERIALIZED toDateTime(x), s String)
ENGINE = MergeTree ORDER BY s TTL m + INTERVAL 1 SECOND
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_mat_source_ttl SELECT 2000000000, 'row' || toString(number) FROM numbers(4);

ALTER TABLE t_mat_source_ttl MODIFY COLUMN x UInt16 SETTINGS mutations_sync = 2;

SELECT count() FROM t_mat_source_ttl;

DROP TABLE t_mat_source_ttl;

-- Case 9: a MATERIALIZED column reading another MATERIALIZED column must be recalculated after it, so the
-- whole chain stays consistent with the new values.
DROP TABLE IF EXISTS t_mat_source_chain;

CREATE TABLE t_mat_source_chain (x Int64, m1 Int64 MATERIALIZED x, m2 Int64 MATERIALIZED m1 * 2)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_mat_source_chain VALUES (5000000000);

ALTER TABLE t_mat_source_chain MODIFY COLUMN x Int32 SETTINGS mutations_sync = 2;

SELECT m1 = x, m2 = m1 * 2 FROM t_mat_source_chain;

DROP TABLE t_mat_source_chain;

-- Case 10: the source column can be reached through an ALIAS column, including a chain of them. The
-- expression is resolved to the columns it is stored in, so the mutation still recalculates it.
DROP TABLE IF EXISTS t_mat_source_alias;

CREATE TABLE t_mat_source_alias (x Int64, a Int64 ALIAS x, b Int64 ALIAS a, m Int64 MATERIALIZED b)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_mat_source_alias VALUES (5000000000);

ALTER TABLE t_mat_source_alias MODIFY COLUMN x Int32 SETTINGS mutations_sync = 2;

SELECT x, m, m = x FROM t_mat_source_alias;

-- ALTER UPDATE of the source column recalculates it through the same alias chain.
ALTER TABLE t_mat_source_alias UPDATE x = 42 WHERE 1 SETTINGS mutations_sync = 2;

SELECT x, m, m = x FROM t_mat_source_alias;

DROP TABLE t_mat_source_alias;

-- Case 11: a MATERIALIZED column in the sorting key that reads the altered column through an ALIAS is
-- rejected as well.
DROP TABLE IF EXISTS t_mat_source_alias_key;

CREATE TABLE t_mat_source_alias_key (x Int64, a Int64 ALIAS x, m Int64 MATERIALIZED a)
ENGINE = MergeTree ORDER BY m SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_mat_source_alias_key SELECT 2147483640 + number FROM numbers(10);

ALTER TABLE t_mat_source_alias_key MODIFY COLUMN x Int32; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT countIf(m = x) FROM t_mat_source_alias_key;

DROP TABLE t_mat_source_alias_key;

-- Case 12: a projection whose WHERE reads the altered column through an ALIAS must be rebuilt too.
DROP TABLE IF EXISTS t_projection_where_alias;

CREATE TABLE t_projection_where_alias (id UInt64, x Int64, neg UInt8 ALIAS x < 0, PROJECTION p (SELECT sum(id) WHERE neg))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_projection_where_alias SELECT number, toInt64(3000000000) + number FROM numbers(100);

ALTER TABLE t_projection_where_alias MODIFY COLUMN x Int32 SETTINGS mutations_sync = 2;

SELECT sum(id) FROM t_projection_where_alias WHERE x < 0 SETTINGS optimize_use_projections = 1;
SELECT sum(id) FROM t_projection_where_alias WHERE x < 0 SETTINGS optimize_use_projections = 0;

DROP TABLE t_projection_where_alias;

-- Case 13: changing a JSON type hint changes the type of a path, so a MATERIALIZED column reading that
-- path is recalculated as well. Reading `007` as Int64 and back as String gives `7`.
DROP TABLE IF EXISTS t_mat_source_json;

CREATE TABLE t_mat_source_json (json JSON(a String), m String MATERIALIZED json.a::String)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_mat_source_json VALUES ('{"a" : "007"}');

SELECT m FROM t_mat_source_json;

ALTER TABLE t_mat_source_json MODIFY COLUMN json JSON(a Int64) SETTINGS mutations_sync = 2;

SELECT m, json.a::String, m = json.a::String FROM t_mat_source_json;

DROP TABLE t_mat_source_json;

-- Case 14: the same change applied as metadata only (a lazy type hint) runs no mutation, so it cannot
-- recalculate the MATERIALIZED column and must be rejected instead of leaving it stale.
DROP TABLE IF EXISTS t_mat_source_json_lazy;

CREATE TABLE t_mat_source_json_lazy (json JSON(a String), m String MATERIALIZED json.a::String)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_mat_source_json_lazy VALUES ('{"a" : "007"}');

SET allow_experimental_json_lazy_type_hints = 1;

ALTER TABLE t_mat_source_json_lazy MODIFY COLUMN json JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SET allow_experimental_json_lazy_type_hints = 0;

SELECT m FROM t_mat_source_json_lazy;

DROP TABLE t_mat_source_json_lazy;

-- Case 15: the key of an aggregate projection is built over the projection's own columns, so resolving it
-- against the table columns fails to find them and breaks the mutation.
DROP TABLE IF EXISTS t_aggregate_projection_key;

CREATE TABLE t_aggregate_projection_key (id UInt64, ts DateTime,
    PROJECTION p (SELECT toStartOfMinute(ts), count() GROUP BY toStartOfMinute(ts)))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_aggregate_projection_key SELECT number, toDateTime('2026-01-01 00:00:00') + number FROM numbers(100);

ALTER TABLE t_aggregate_projection_key MODIFY COLUMN ts DateTime64(3) SETTINGS mutations_sync = 2;

SELECT count() FROM (SELECT toStartOfMinute(ts) AS k, count() FROM t_aggregate_projection_key GROUP BY k)
SETTINGS optimize_use_projections = 1;
SELECT count() FROM (SELECT toStartOfMinute(ts) AS k, count() FROM t_aggregate_projection_key GROUP BY k)
SETTINGS optimize_use_projections = 0;

DROP TABLE t_aggregate_projection_key;

-- Case 16: the altered column reaches the key MATERIALIZED column through another MATERIALIZED column,
-- which is just as unrecalculable in place, so the ALTER is rejected as well.
DROP TABLE IF EXISTS t_mat_source_key_chain;

CREATE TABLE t_mat_source_key_chain (x Int64, m1 Int64 MATERIALIZED x, m2 Int64 MATERIALIZED m1 * 2)
ENGINE = MergeTree ORDER BY m2 SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_mat_source_key_chain SELECT 2147483640 + number FROM numbers(10);

ALTER TABLE t_mat_source_key_chain MODIFY COLUMN x Int32; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT countIf(m1 = x AND m2 = m1 * 2) FROM t_mat_source_key_chain;

DROP TABLE t_mat_source_key_chain;

-- Case 17: a TTL reading the altered column directly must be recalculated too, so rows whose TTL the
-- conversion moves into the past expire.
DROP TABLE IF EXISTS t_ttl_source_type;

CREATE TABLE t_ttl_source_type (x UInt64, s String) ENGINE = MergeTree ORDER BY s
TTL toDateTime(x) + INTERVAL 1 SECOND SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_ttl_source_type SELECT 2000000000, 'row' || toString(number) FROM numbers(4);

SELECT count() FROM t_ttl_source_type;

ALTER TABLE t_ttl_source_type MODIFY COLUMN x UInt16 SETTINGS mutations_sync = 2;

SELECT count() FROM t_ttl_source_type;

DROP TABLE t_ttl_source_type;
