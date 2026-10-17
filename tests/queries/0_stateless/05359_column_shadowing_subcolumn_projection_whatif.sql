-- A column `a.size0` added next to an `Array` column `a` is not provided by a projection that stores only `a`,
-- so EXPLAIN WHATIF must not report the projection as applicable for it.

DROP TABLE IF EXISTS t_whatif_shadow;
CREATE TABLE t_whatif_shadow (k UInt64, a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_whatif_shadow SELECT number, range(number % 5) FROM numbers(100);
ALTER TABLE t_whatif_shadow ADD COLUMN `a.size0` UInt64;

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, enable_parallel_replicas = 0;

CREATE HYPOTHETICAL PROJECTION p_k ON t_whatif_shadow (SELECT k, a ORDER BY k);

SELECT '-- the column a.size0';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT k, `a.size0` FROM t_whatif_shadow WHERE k = 42)
WHERE match(line, '^(status|reason):');

SELECT '-- the column a';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT k, a FROM t_whatif_shadow WHERE k = 42)
WHERE match(line, '^status:');

DROP TABLE t_whatif_shadow;
