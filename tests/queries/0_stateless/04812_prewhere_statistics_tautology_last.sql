-- Regression: a predicate that rejects no rows (estimated_row_count == total_rows) is useless in
-- PREWHERE and must be scheduled last, even when its column is cheap. Otherwise the cost-per-rejected-row
-- score gives it a finite value (columns_size) and a cheap tautology can be placed before a genuinely
-- selective but expensive predicate.
-- See https://github.com/ClickHouse/ClickHouse/pull/110695

SET enable_analyzer = 1;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET allow_reorder_prewhere_conditions = 1; -- CI may inject False, preventing statistics-based reordering
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_prewhere_tautology;

-- Wide parts so per-column sizes feed the cost score. tdigest on `cheap` lets the optimizer see
-- `cheap < 255` as a tautology (matches all rows); `expensive = 'absent'` has no statistic, so its
-- default equality selectivity makes it the selective but expensive (large incompressible values) predicate.
CREATE TABLE t_prewhere_tautology (
    cheap UInt8 STATISTICS(tdigest),
    expensive String
) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = '';

INSERT INTO t_prewhere_tautology SELECT 1, randomPrintableASCII(4000) FROM numbers(20000);

SELECT '-- tautology (rejects nothing) is scheduled after the selective expensive predicate';
SELECT position(prewhere_line, 'equals(expensive') > 0
   AND position(prewhere_line, 'equals(expensive') < position(prewhere_line, 'less(cheap') AS selective_first
FROM (
    SELECT extractAll(replaceRegexpAll(explain, '__table1\.', ''), 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions = 1 SELECT count() FROM t_prewhere_tautology WHERE cheap < 255 AND expensive = 'absent'
    ) WHERE explain LIKE '%Prewhere filter column%'
);

SELECT '-- correctness: result is unchanged';
SELECT count() FROM t_prewhere_tautology WHERE cheap < 255 AND expensive = 'absent';

DROP TABLE t_prewhere_tautology;
