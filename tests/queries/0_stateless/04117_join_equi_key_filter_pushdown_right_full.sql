-- Equi-key `WHERE` predicates must reach the left `MergeTree` input of a `RIGHT JOIN` as an index
-- condition. Previously this only happened for `Semi` strictness.
--
-- Nothing may be pushed through a `FULL JOIN`: dropping a row from either input only turns its
-- counterpart into a defaulted unmatched row, so a predicate on that default both admits rows
-- belonging to neither input and discards rows that should survive. The `FULL JOIN` cases assert
-- results only, guarding against reintroducing such a push.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_join_swap_table = 'false';
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS mt;
CREATE TABLE mt (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO mt SELECT number FROM numbers(1000000);

SELECT 'RIGHT JOIN USING, equi-key WHERE, matched types: left MergeTree prunes granules';
SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT k FROM mt AS l RIGHT JOIN (SELECT toUInt64(1) AS k) AS r USING (k) WHERE k = 1
) WHERE explain ILIKE '%Condition: (k in [1, 1])%';

SELECT 'RIGHT JOIN USING, equi-key WHERE, UInt8/UInt64 mismatch: left MergeTree prunes granules';
SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT k FROM mt AS l RIGHT JOIN (SELECT 1 AS k) AS r USING (k) WHERE k = 1
) WHERE explain ILIKE '%Condition: (k in [1, 1])%';

SELECT 'RIGHT JOIN USING, equi-key WHERE: result';
SELECT k FROM mt AS l RIGHT JOIN (SELECT 1 AS k) AS r USING (k) WHERE k = 1 ORDER BY k;

SELECT 'RIGHT ALL JOIN ON, equi-key WHERE: left MergeTree prunes granules';
SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT l.k FROM mt AS l RIGHT JOIN (SELECT toUInt64(1) AS k) AS r ON l.k = r.k WHERE r.k = 1
) WHERE explain ILIKE '%Condition: (k in [1, 1])%';

SELECT 'RIGHT ANTI JOIN USING, equi-key WHERE: correctness preserved';
SELECT k FROM mt AS l RIGHT ANTI JOIN (SELECT toUInt64(0) AS k UNION ALL SELECT toUInt64(1000000000) AS k) AS r USING (k) WHERE k = 1000000000 ORDER BY k;

SELECT 'FULL JOIN USING, equi-key WHERE: result (matched row)';
SELECT k FROM mt AS l FULL JOIN (SELECT toUInt64(1) AS k) AS r USING (k) WHERE k = 1 ORDER BY k;

SELECT 'FULL JOIN USING, equi-key WHERE: result (right-only contributing row preserved)';
SELECT k FROM mt AS l FULL JOIN (SELECT toUInt64(1000000000) AS k) AS r USING (k) WHERE k = 1000000000 ORDER BY k;

SELECT 'FULL JOIN USING, equi-key WHERE: left-only rows still reachable via filter';
SELECT count() FROM (
    SELECT k FROM mt AS l FULL JOIN (SELECT toUInt64(1000000000) AS k) AS r USING (k) WHERE k = 7
);

DROP TABLE mt;

DROP TABLE IF EXISTS s1;
DROP TABLE IF EXISTS s2;
CREATE TABLE s1 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE s2 (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO s1 VALUES (5);
INSERT INTO s2 VALUES (3);

-- Pushing this would drop the only left row and lose the `(5, 0)` unmatched row it produces.
SELECT 'FULL JOIN, side-qualified equi-key predicate: unmatched rows preserved';
SELECT lhs.id, rhs.id FROM s1 AS lhs FULL JOIN s2 AS rhs ON lhs.id = rhs.id WHERE rhs.id != 5 ORDER BY 1, 2;

DROP TABLE s1;
DROP TABLE s2;

DROP TABLE IF EXISTS u1;
DROP TABLE IF EXISTS u2;
CREATE TABLE u1 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE u2 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO u1 VALUES (1, 'Value_1'), (2, 'Value_2');
INSERT INTO u2 VALUES (2, 'Value_2'), (3, 'Value_3');

-- Opposite direction: pushing these to both inputs and dropping them from the post-join filter would
-- let the two defaulted unmatched rows escape.
SELECT 'FULL JOIN, side-qualified equi-key predicates on both sides: only the matched row survives';
SELECT * FROM u1 AS lhs FULL JOIN u2 AS rhs ON lhs.id = rhs.id WHERE lhs.id != 0 AND rhs.id != 0 ORDER BY 1, 3;

DROP TABLE u1;
DROP TABLE u2;
