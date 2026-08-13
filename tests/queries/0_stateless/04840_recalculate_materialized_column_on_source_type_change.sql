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
