-- A predicate the estimator cannot analyse (here a comparison between two columns) becomes an unknown
-- atom and is given `default_unknown_cond_factor`. When two such atoms were AND-ed, the merge of the
-- conjunction absorbed them: merging carries only ranges, an unknown atom has none, so the merged
-- clause ended up with no ranges at all and finalized to a selectivity of 1 - the estimate degraded to
-- the full table exactly when the query had *more* conditions.

DROP TABLE IF EXISTS t_unknown_atoms;
DROP TABLE IF EXISTS t_unknown_dim;

CREATE TABLE t_unknown_atoms (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_unknown_dim (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = 'basic, uniq_v2';

-- Statistics are materialized by a merge, so two parts and an OPTIMIZE are required.
INSERT INTO t_unknown_atoms SELECT number, number, number FROM numbers(50000);
INSERT INTO t_unknown_atoms SELECT number + 50000, number, number FROM numbers(50000);
INSERT INTO t_unknown_dim SELECT number FROM numbers(100);
OPTIMIZE TABLE t_unknown_atoms FINAL;

SELECT 'statistics materialized', max(level) >= 1
FROM system.parts WHERE database = currentDatabase() AND table = 't_unknown_atoms' AND active;

-- One unknown atom: estimated below the table size.
SELECT 'one unknown atom is below the total',
       toUInt64OrZero(extract(explain, 't_unknown_atoms\\[(\\d+)\\]')) < 100000
FROM
(
    EXPLAIN PLAN keep_logical_steps = 1, actions = 1
    SELECT count() FROM t_unknown_atoms INNER JOIN t_unknown_dim ON t_unknown_dim.id = t_unknown_atoms.a
    WHERE t_unknown_atoms.b > t_unknown_atoms.c
    SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0
)
WHERE explain LIKE '%Join:%';

-- Two unknown atoms: must also be below the table size, and no larger than one atom alone.
SELECT 'two unknown atoms are below the total',
       toUInt64OrZero(extract(explain, 't_unknown_atoms\\[(\\d+)\\]')) < 100000
FROM
(
    EXPLAIN PLAN keep_logical_steps = 1, actions = 1
    SELECT count() FROM t_unknown_atoms INNER JOIN t_unknown_dim ON t_unknown_dim.id = t_unknown_atoms.a
    WHERE t_unknown_atoms.b > t_unknown_atoms.c AND t_unknown_atoms.a > t_unknown_atoms.c
    SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0
)
WHERE explain LIKE '%Join:%';

DROP TABLE t_unknown_atoms;
DROP TABLE t_unknown_dim;
