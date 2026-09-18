-- Tests the multi-range (interval cover) form of enable_join_runtime_filters_index_analysis.
-- When the build side overflows the exact-values limit the runtime filter falls back to its
-- recorded key ranges. A build side that sits in two clusters far apart must produce two
-- intervals, so that the left side keeps only the granules around those clusters instead of
-- everything between the global minimum and maximum.

DROP TABLE IF EXISTS mr_fact;
DROP TABLE IF EXISTS mr_dim;

CREATE TABLE mr_fact (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16;
CREATE TABLE mr_dim (id UInt64, tag String) ENGINE = MergeTree ORDER BY id;

INSERT INTO mr_fact SELECT number, number FROM numbers(40000);
-- Two clusters at the ends of the key domain: [0, 4000) and [36000, 40000).
INSERT INTO mr_dim SELECT number, 'hot' FROM numbers(4000);
INSERT INTO mr_dim SELECT 36000 + number, 'hot' FROM numbers(4000);

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET use_skip_indexes_on_data_read = 1;
SET join_runtime_filter_min_probe_rows = 0;
-- The asserted granule counts depend on which side builds the runtime filter.
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
-- Left-side join pruning is intentionally disabled under parallel replicas.
SET enable_parallel_replicas = 0;
-- Force the range path: with the default limit the 8000 distinct keys would fit in the exact
-- IN-set, which prunes on its own and never exercises the cover.
SET join_runtime_filter_exact_values_limit = 100;

SELECT count(), sum(f.v)
FROM mr_fact AS f
INNER JOIN mr_dim AS d ON f.id = d.id
FORMAT Null
SETTINGS log_comment = '05023_cover';

SYSTEM FLUSH LOGS query_log;

-- A single [min, max] envelope spans the whole table and drops nothing. Two intervals drop the
-- ~80% of granules that sit in the gap between the clusters.
SELECT
    argMax(ProfileEvents['RuntimeFilterGranulesConsidered'], event_time) > 0,
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time) * 2
        > argMax(ProfileEvents['RuntimeFilterGranulesConsidered'], event_time)
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '05023_cover'
    AND type = 'QueryFinish';

-- The keys are recorded as bits in a histogram, so the two clusters are found even when the build
-- side is shuffled and every block spans the whole key domain -- the case that one interval per
-- block cannot see. The intervals come out bucket-aligned, hence slightly wider, but never narrower.
DROP TABLE IF EXISTS mr_dim_shuffled;
CREATE TABLE mr_dim_shuffled (id UInt64, tag String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO mr_dim_shuffled SELECT if(number % 2 = 0, intDiv(number, 2), 36000 + intDiv(number, 2)), 'hot' FROM numbers(8000);

SELECT count(), sum(f.v)
FROM mr_fact AS f
INNER JOIN mr_dim_shuffled AS d ON f.id = d.id
FORMAT Null
SETTINGS log_comment = '05023_shuffled';

SYSTEM FLUSH LOGS query_log;

SELECT
    argMax(ProfileEvents['RuntimeFilterGranulesConsidered'], event_time) > 0,
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time) * 2
        > argMax(ProfileEvents['RuntimeFilterGranulesConsidered'], event_time)
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '05023_shuffled'
    AND type = 'QueryFinish';

SELECT
    (SELECT count() FROM mr_fact AS f INNER JOIN mr_dim_shuffled AS d ON f.id = d.id
     SETTINGS enable_join_runtime_filters_index_analysis = 0) =
    (SELECT count() FROM mr_fact AS f INNER JOIN mr_dim_shuffled AS d ON f.id = d.id
     SETTINGS enable_join_runtime_filters_index_analysis = 1);

-- Pruning must not change the result.
SELECT
    (SELECT count() FROM mr_fact AS f INNER JOIN mr_dim AS d ON f.id = d.id
     SETTINGS enable_join_runtime_filters_index_analysis = 0) =
    (SELECT count() FROM mr_fact AS f INNER JOIN mr_dim AS d ON f.id = d.id
     SETTINGS enable_join_runtime_filters_index_analysis = 1);

-- A build side spread evenly over the domain has no gaps worth splitting, so the cover collapses
-- back to one envelope. The point of the check is that the result stays correct either way.
SELECT
    (SELECT count() FROM mr_fact AS f INNER JOIN mr_dim AS d ON f.id = d.id
     WHERE d.id % 7 = 0 SETTINGS enable_join_runtime_filters_index_analysis = 0) =
    (SELECT count() FROM mr_fact AS f INNER JOIN mr_dim AS d ON f.id = d.id
     WHERE d.id % 7 = 0 SETTINGS enable_join_runtime_filters_index_analysis = 1);

-- A Nullable key takes the exact-set filter, which drops keys once it overflows and merges its
-- recorded ranges separately from its values. If the merge lost part of a parallel build stream's
-- range, the left side would be pruned by a range that is too narrow and rows would go missing.
DROP TABLE IF EXISTS mr_fact_null;
DROP TABLE IF EXISTS mr_dim_null;

CREATE TABLE mr_fact_null (id Nullable(UInt64), v UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16, allow_nullable_key = 1;
CREATE TABLE mr_dim_null (id Nullable(UInt64), tag String) ENGINE = MergeTree ORDER BY id SETTINGS allow_nullable_key = 1;

INSERT INTO mr_fact_null SELECT number, number FROM numbers(40000);
INSERT INTO mr_dim_null SELECT number, 'hot' FROM numbers(4000);
INSERT INTO mr_dim_null SELECT 36000 + number, 'hot' FROM numbers(4000);

SELECT
    (SELECT count() FROM mr_fact_null AS f INNER JOIN mr_dim_null AS d ON f.id = d.id
     SETTINGS enable_join_runtime_filters_index_analysis = 0, max_threads = 4) =
    (SELECT count() FROM mr_fact_null AS f INNER JOIN mr_dim_null AS d ON f.id = d.id
     SETTINGS enable_join_runtime_filters_index_analysis = 1, max_threads = 4);

-- Keys at the very top of the UInt64 and Int64 domains. The bucket grid can extend past the end of
-- the coordinate space, and a wrapped high edge would invert an interval, which prunes every granule
-- and drops matching rows. The two clusters are chosen so the grid does not land on the last key.
DROP TABLE IF EXISTS mr_fact_top;
DROP TABLE IF EXISTS mr_dim_top;
DROP TABLE IF EXISTS mr_fact_top_signed;
DROP TABLE IF EXISTS mr_dim_top_signed;

CREATE TABLE mr_fact_top (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 16;
CREATE TABLE mr_dim_top (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO mr_fact_top SELECT 18446744073709551615 - number FROM numbers(40001);
INSERT INTO mr_dim_top SELECT 18446744073709551615 - number FROM numbers(4000);
INSERT INTO mr_dim_top SELECT 18446744073709511615 + number FROM numbers(4000);

CREATE TABLE mr_fact_top_signed (k Int64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 16;
CREATE TABLE mr_dim_top_signed (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO mr_fact_top_signed SELECT 9223372036854775807 - number FROM numbers(40001);
INSERT INTO mr_dim_top_signed SELECT 9223372036854775807 - number FROM numbers(4000);
INSERT INTO mr_dim_top_signed SELECT 9223372036854735807 + number FROM numbers(4000);

SELECT
    (SELECT count() FROM mr_fact_top AS f INNER JOIN mr_dim_top AS d ON f.k = d.k
     SETTINGS enable_join_runtime_filters_index_analysis = 0) =
    (SELECT count() FROM mr_fact_top AS f INNER JOIN mr_dim_top AS d ON f.k = d.k
     SETTINGS enable_join_runtime_filters_index_analysis = 1);

SELECT
    (SELECT count() FROM mr_fact_top_signed AS f INNER JOIN mr_dim_top_signed AS d ON f.k = d.k
     SETTINGS enable_join_runtime_filters_index_analysis = 0) =
    (SELECT count() FROM mr_fact_top_signed AS f INNER JOIN mr_dim_top_signed AS d ON f.k = d.k
     SETTINGS enable_join_runtime_filters_index_analysis = 1);

DROP TABLE mr_fact;
DROP TABLE mr_dim;
DROP TABLE mr_dim_shuffled;
DROP TABLE mr_fact_top;
DROP TABLE mr_dim_top;
DROP TABLE mr_fact_top_signed;
DROP TABLE mr_dim_top_signed;
DROP TABLE mr_fact_null;
DROP TABLE mr_dim_null;
