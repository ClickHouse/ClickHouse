-- Tags: long

-- Differential (oracle) test for `use_partition_minmax_for_primary_key_pruning`.
--
-- Index analysis is a pure optimization: it may only skip data, never change results. So for every
-- generated predicate the result must be identical across all index-analysis settings and a full scan
-- with all indexes disabled (the oracle). Each scenario reports a single mismatch counter (expected 0)
-- and a failing-seeds query (expected empty) that prints the offending predicate seeds on failure.
-- A directional check via `EXPLAIN ESTIMATE` asserts the optimization never selects more marks than
-- with the setting off, and selects strictly fewer for a known-selective predicate (so a silently
-- disabled optimization cannot pass vacuously).
--
-- Predicates are generated deterministically from a seed `n`: constants sweep across the partition
-- boundary (1000), where the partition-minmax bounds bite, mixing =, >=, <, BETWEEN, OR and NULLs.
-- The result of each variant is folded into one scalar, `cityHash64(count(), sum(cityHash64(...)))`,
-- so both lost rows and changed rows are detected.

SET optimize_trivial_count_query = 0;
SET optimize_use_implicit_projections = 0;
SET use_query_condition_cache = 0;

-- A) Suffix key column loaded in the in-memory index.
DROP TABLE IF EXISTS t_pmm_oracle_loaded;
CREATE TABLE t_pmm_oracle_loaded (event_time UInt32, id UInt32)
ENGINE = MergeTree PARTITION BY intDiv(event_time, 1000) ORDER BY (id, event_time)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 1;
INSERT INTO t_pmm_oracle_loaded SELECT number % 1200, number % 10 FROM numbers(1200);

DROP VIEW IF EXISTS matrix_loaded;
CREATE VIEW matrix_loaded AS
SELECT
    n,
    (SELECT cityHash64(count(), sum(cityHash64(id, event_time))) FROM t_pmm_oracle_loaded
     WHERE (id = n % 10 AND event_time = (n * 83) % 1200)
        OR (id >= 4 + n % 7 AND event_time > 890 + (n * 37) % 220)
        OR (id < n % 3 AND event_time BETWEEN (n * 59) % 1100 AND (n * 59) % 1100 + n)
     SETTINGS use_partition_minmax_for_primary_key_pruning = 1, use_lightweight_primary_key_index_analysis = 1) AS a,
    (SELECT cityHash64(count(), sum(cityHash64(id, event_time))) FROM t_pmm_oracle_loaded
     WHERE (id = n % 10 AND event_time = (n * 83) % 1200)
        OR (id >= 4 + n % 7 AND event_time > 890 + (n * 37) % 220)
        OR (id < n % 3 AND event_time BETWEEN (n * 59) % 1100 AND (n * 59) % 1100 + n)
     SETTINGS use_partition_minmax_for_primary_key_pruning = 1, use_lightweight_primary_key_index_analysis = 0) AS b,
    (SELECT cityHash64(count(), sum(cityHash64(id, event_time))) FROM t_pmm_oracle_loaded
     WHERE (id = n % 10 AND event_time = (n * 83) % 1200)
        OR (id >= 4 + n % 7 AND event_time > 890 + (n * 37) % 220)
        OR (id < n % 3 AND event_time BETWEEN (n * 59) % 1100 AND (n * 59) % 1100 + n)
     SETTINGS use_partition_minmax_for_primary_key_pruning = 0, use_lightweight_primary_key_index_analysis = 1) AS c,
    (SELECT cityHash64(count(), sum(cityHash64(id, event_time))) FROM t_pmm_oracle_loaded
     WHERE (id = n % 10 AND event_time = (n * 83) % 1200)
        OR (id >= 4 + n % 7 AND event_time > 890 + (n * 37) % 220)
        OR (id < n % 3 AND event_time BETWEEN (n * 59) % 1100 AND (n * 59) % 1100 + n)
     SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0) AS d
FROM (SELECT number AS n FROM numbers(25));

SELECT 'A mismatches', countIf(a != d OR b != d OR c != d) FROM matrix_loaded;
SELECT 'A failing seeds', n, a, b, c, d FROM matrix_loaded WHERE a != d OR b != d OR c != d;

SELECT 'A pruning not worse',
    (SELECT sum(marks) FROM viewExplain('EXPLAIN ESTIMATE', '', (
        SELECT count() FROM t_pmm_oracle_loaded WHERE (id = 3 AND event_time = 10) OR (id >= 7 AND event_time > 1100)
        SETTINGS use_partition_minmax_for_primary_key_pruning = 1)))
    <=
    (SELECT sum(marks) FROM viewExplain('EXPLAIN ESTIMATE', '', (
        SELECT count() FROM t_pmm_oracle_loaded WHERE (id = 3 AND event_time = 10) OR (id >= 7 AND event_time > 1100)
        SETTINGS use_partition_minmax_for_primary_key_pruning = 0)));

SELECT 'A pruning fires',
    (SELECT sum(marks) FROM viewExplain('EXPLAIN ESTIMATE', '', (
        SELECT count() FROM t_pmm_oracle_loaded WHERE (id = 3 AND event_time = 10) OR (id >= 7 AND event_time > 1100)
        SETTINGS use_partition_minmax_for_primary_key_pruning = 1)))
    <
    (SELECT sum(marks) FROM viewExplain('EXPLAIN ESTIMATE', '', (
        SELECT count() FROM t_pmm_oracle_loaded WHERE (id = 3 AND event_time = 10) OR (id >= 7 AND event_time > 1100)
        SETTINGS use_partition_minmax_for_primary_key_pruning = 0)));

DROP VIEW matrix_loaded;
DROP TABLE t_pmm_oracle_loaded;

-- B) Suffix key column dropped from the in-memory index (high-cardinality prefix): the bound is applied
--    as a constant coordinate on the sparse representation.
DROP TABLE IF EXISTS t_pmm_oracle_dropped;
CREATE TABLE t_pmm_oracle_dropped (event_time UInt32, id UInt32)
ENGINE = MergeTree PARTITION BY intDiv(event_time, 1000) ORDER BY (id, event_time)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.01;
INSERT INTO t_pmm_oracle_dropped SELECT number % 1200, number % 120 FROM numbers(1200);

DROP VIEW IF EXISTS matrix_dropped;
CREATE VIEW matrix_dropped AS
SELECT
    n,
    (SELECT cityHash64(count(), sum(cityHash64(id, event_time))) FROM t_pmm_oracle_dropped
     WHERE (id = (n * 13) % 120 AND event_time = (n * 83) % 1200)
        OR (id >= 90 + n % 29 AND event_time > 890 + (n * 37) % 220)
        OR (id < n % 5 AND event_time BETWEEN (n * 59) % 1100 AND (n * 59) % 1100 + n)
     SETTINGS use_partition_minmax_for_primary_key_pruning = 1, use_lightweight_primary_key_index_analysis = 1) AS a,
    (SELECT cityHash64(count(), sum(cityHash64(id, event_time))) FROM t_pmm_oracle_dropped
     WHERE (id = (n * 13) % 120 AND event_time = (n * 83) % 1200)
        OR (id >= 90 + n % 29 AND event_time > 890 + (n * 37) % 220)
        OR (id < n % 5 AND event_time BETWEEN (n * 59) % 1100 AND (n * 59) % 1100 + n)
     SETTINGS use_partition_minmax_for_primary_key_pruning = 1, use_lightweight_primary_key_index_analysis = 0) AS b,
    (SELECT cityHash64(count(), sum(cityHash64(id, event_time))) FROM t_pmm_oracle_dropped
     WHERE (id = (n * 13) % 120 AND event_time = (n * 83) % 1200)
        OR (id >= 90 + n % 29 AND event_time > 890 + (n * 37) % 220)
        OR (id < n % 5 AND event_time BETWEEN (n * 59) % 1100 AND (n * 59) % 1100 + n)
     SETTINGS use_partition_minmax_for_primary_key_pruning = 0, use_lightweight_primary_key_index_analysis = 1) AS c,
    (SELECT cityHash64(count(), sum(cityHash64(id, event_time))) FROM t_pmm_oracle_dropped
     WHERE (id = (n * 13) % 120 AND event_time = (n * 83) % 1200)
        OR (id >= 90 + n % 29 AND event_time > 890 + (n * 37) % 220)
        OR (id < n % 5 AND event_time BETWEEN (n * 59) % 1100 AND (n * 59) % 1100 + n)
     SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0) AS d
FROM (SELECT number AS n FROM numbers(25));

SELECT 'B mismatches', countIf(a != d OR b != d OR c != d) FROM matrix_dropped;
SELECT 'B failing seeds', n, a, b, c, d FROM matrix_dropped WHERE a != d OR b != d OR c != d;

SELECT 'B pruning not worse',
    (SELECT sum(marks) FROM viewExplain('EXPLAIN ESTIMATE', '', (
        SELECT count() FROM t_pmm_oracle_dropped WHERE (id = 3 AND event_time = 10) OR (id >= 100 AND event_time > 1100)
        SETTINGS use_partition_minmax_for_primary_key_pruning = 1)))
    <=
    (SELECT sum(marks) FROM viewExplain('EXPLAIN ESTIMATE', '', (
        SELECT count() FROM t_pmm_oracle_dropped WHERE (id = 3 AND event_time = 10) OR (id >= 100 AND event_time > 1100)
        SETTINGS use_partition_minmax_for_primary_key_pruning = 0)));

SELECT 'B pruning fires',
    (SELECT sum(marks) FROM viewExplain('EXPLAIN ESTIMATE', '', (
        SELECT count() FROM t_pmm_oracle_dropped WHERE (id = 3 AND event_time = 10) OR (id >= 100 AND event_time > 1100)
        SETTINGS use_partition_minmax_for_primary_key_pruning = 1)))
    <
    (SELECT sum(marks) FROM viewExplain('EXPLAIN ESTIMATE', '', (
        SELECT count() FROM t_pmm_oracle_dropped WHERE (id = 3 AND event_time = 10) OR (id >= 100 AND event_time > 1100)
        SETTINGS use_partition_minmax_for_primary_key_pruning = 0)));

DROP VIEW matrix_dropped;
DROP TABLE t_pmm_oracle_dropped;

-- C) Nullable leading key column with NULLs (NULL_LAST ordering) combined with a dropped, bounded suffix:
--    the last-mark handling maps NULL to +inf while the suffix uses its constant partition-minmax bound.
DROP TABLE IF EXISTS t_pmm_oracle_nullable;
CREATE TABLE t_pmm_oracle_nullable (event_time UInt32, id Nullable(UInt32))
ENGINE = MergeTree PARTITION BY intDiv(event_time, 1000) ORDER BY (id, event_time)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0, allow_nullable_key = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.01;
INSERT INTO t_pmm_oracle_nullable SELECT number % 1200, if(number % 7 = 0, NULL, number % 120) FROM numbers(1200);

DROP VIEW IF EXISTS matrix_nullable;
CREATE VIEW matrix_nullable AS
SELECT
    n,
    (SELECT cityHash64(count(), sum(cityHash64(coalesce(id, 4294967295), event_time))) FROM t_pmm_oracle_nullable
     WHERE (id = (n * 13) % 120 AND event_time = (n * 83) % 1200)
        OR (id >= 90 + n % 29 AND event_time > 890 + (n * 37) % 220)
        OR (id IS NULL AND event_time > (n * 41) % 1150)
     SETTINGS use_partition_minmax_for_primary_key_pruning = 1, use_lightweight_primary_key_index_analysis = 1) AS a,
    (SELECT cityHash64(count(), sum(cityHash64(coalesce(id, 4294967295), event_time))) FROM t_pmm_oracle_nullable
     WHERE (id = (n * 13) % 120 AND event_time = (n * 83) % 1200)
        OR (id >= 90 + n % 29 AND event_time > 890 + (n * 37) % 220)
        OR (id IS NULL AND event_time > (n * 41) % 1150)
     SETTINGS use_partition_minmax_for_primary_key_pruning = 1, use_lightweight_primary_key_index_analysis = 0) AS b,
    (SELECT cityHash64(count(), sum(cityHash64(coalesce(id, 4294967295), event_time))) FROM t_pmm_oracle_nullable
     WHERE (id = (n * 13) % 120 AND event_time = (n * 83) % 1200)
        OR (id >= 90 + n % 29 AND event_time > 890 + (n * 37) % 220)
        OR (id IS NULL AND event_time > (n * 41) % 1150)
     SETTINGS use_partition_minmax_for_primary_key_pruning = 0, use_lightweight_primary_key_index_analysis = 1) AS c,
    (SELECT cityHash64(count(), sum(cityHash64(coalesce(id, 4294967295), event_time))) FROM t_pmm_oracle_nullable
     WHERE (id = (n * 13) % 120 AND event_time = (n * 83) % 1200)
        OR (id >= 90 + n % 29 AND event_time > 890 + (n * 37) % 220)
        OR (id IS NULL AND event_time > (n * 41) % 1150)
     SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0) AS d
FROM (SELECT number AS n FROM numbers(25));

SELECT 'C mismatches', countIf(a != d OR b != d OR c != d) FROM matrix_nullable;
SELECT 'C failing seeds', n, a, b, c, d FROM matrix_nullable WHERE a != d OR b != d OR c != d;

SELECT 'C pruning not worse',
    (SELECT sum(marks) FROM viewExplain('EXPLAIN ESTIMATE', '', (
        SELECT count() FROM t_pmm_oracle_nullable WHERE (id = 3 AND event_time = 10) OR (id IS NULL AND event_time > 1100)
        SETTINGS use_partition_minmax_for_primary_key_pruning = 1)))
    <=
    (SELECT sum(marks) FROM viewExplain('EXPLAIN ESTIMATE', '', (
        SELECT count() FROM t_pmm_oracle_nullable WHERE (id = 3 AND event_time = 10) OR (id IS NULL AND event_time > 1100)
        SETTINGS use_partition_minmax_for_primary_key_pruning = 0)));

DROP VIEW matrix_nullable;
DROP TABLE t_pmm_oracle_nullable;

-- D) Reversed (DESC) bounded key column: the partition minmax is stored value-ascending while the index
--    column is reverse-flagged, so the boundary swap logic must not corrupt the bound.
DROP TABLE IF EXISTS t_pmm_oracle_reverse;
CREATE TABLE t_pmm_oracle_reverse (event_time UInt32, id UInt32)
ENGINE = MergeTree PARTITION BY intDiv(event_time, 1000) ORDER BY (id, event_time DESC)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0, allow_experimental_reverse_key = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 1;
INSERT INTO t_pmm_oracle_reverse SELECT number % 1200, number % 10 FROM numbers(1200);

DROP VIEW IF EXISTS matrix_reverse;
CREATE VIEW matrix_reverse AS
SELECT
    n,
    (SELECT cityHash64(count(), sum(cityHash64(id, event_time))) FROM t_pmm_oracle_reverse
     WHERE (id = n % 10 AND event_time = (n * 83) % 1200)
        OR (id >= 4 + n % 7 AND event_time > 890 + (n * 37) % 220)
        OR (id < n % 3 AND event_time BETWEEN (n * 59) % 1100 AND (n * 59) % 1100 + n)
     SETTINGS use_partition_minmax_for_primary_key_pruning = 1, use_lightweight_primary_key_index_analysis = 1) AS a,
    (SELECT cityHash64(count(), sum(cityHash64(id, event_time))) FROM t_pmm_oracle_reverse
     WHERE (id = n % 10 AND event_time = (n * 83) % 1200)
        OR (id >= 4 + n % 7 AND event_time > 890 + (n * 37) % 220)
        OR (id < n % 3 AND event_time BETWEEN (n * 59) % 1100 AND (n * 59) % 1100 + n)
     SETTINGS use_partition_minmax_for_primary_key_pruning = 1, use_lightweight_primary_key_index_analysis = 0) AS b,
    (SELECT cityHash64(count(), sum(cityHash64(id, event_time))) FROM t_pmm_oracle_reverse
     WHERE (id = n % 10 AND event_time = (n * 83) % 1200)
        OR (id >= 4 + n % 7 AND event_time > 890 + (n * 37) % 220)
        OR (id < n % 3 AND event_time BETWEEN (n * 59) % 1100 AND (n * 59) % 1100 + n)
     SETTINGS use_partition_minmax_for_primary_key_pruning = 0, use_lightweight_primary_key_index_analysis = 1) AS c,
    (SELECT cityHash64(count(), sum(cityHash64(id, event_time))) FROM t_pmm_oracle_reverse
     WHERE (id = n % 10 AND event_time = (n * 83) % 1200)
        OR (id >= 4 + n % 7 AND event_time > 890 + (n * 37) % 220)
        OR (id < n % 3 AND event_time BETWEEN (n * 59) % 1100 AND (n * 59) % 1100 + n)
     SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0) AS d
FROM (SELECT number AS n FROM numbers(25));

SELECT 'D mismatches', countIf(a != d OR b != d OR c != d) FROM matrix_reverse;
SELECT 'D failing seeds', n, a, b, c, d FROM matrix_reverse WHERE a != d OR b != d OR c != d;

SELECT 'D pruning not worse',
    (SELECT sum(marks) FROM viewExplain('EXPLAIN ESTIMATE', '', (
        SELECT count() FROM t_pmm_oracle_reverse WHERE (id = 3 AND event_time = 10) OR (id >= 7 AND event_time > 1100)
        SETTINGS use_partition_minmax_for_primary_key_pruning = 1)))
    <=
    (SELECT sum(marks) FROM viewExplain('EXPLAIN ESTIMATE', '', (
        SELECT count() FROM t_pmm_oracle_reverse WHERE (id = 3 AND event_time = 10) OR (id >= 7 AND event_time > 1100)
        SETTINGS use_partition_minmax_for_primary_key_pruning = 0)));

DROP VIEW matrix_reverse;
DROP TABLE t_pmm_oracle_reverse;
