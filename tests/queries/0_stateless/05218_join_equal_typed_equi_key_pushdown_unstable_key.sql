-- A predicate over one side of an equi-join key is pushed to the other side by substituting that side's key
-- expression into it, so the pushed filter evaluates that expression while the JOIN evaluates it again. A key
-- that does not survive a second evaluation - it changes the number of rows, it is stateful, or it is observable
-- beyond its value - must not be substituted, otherwise the answer or what the query accounts for changes.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
-- The plan-shape assertions below describe a fixed side order and a local read.
SET query_plan_join_swap_table = 'false';
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS l_unstable_key;
DROP TABLE IF EXISTS r_unstable_key;

CREATE TABLE l_unstable_key (a Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE r_unstable_key (x Int64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO l_unstable_key SELECT number FROM numbers(10);
INSERT INTO r_unstable_key SELECT number FROM numbers(10);

SELECT 'row-count-changing key';

-- Each row of the right side matches twice, so 2 keys x 2 matches = 4 rows. Substituting the key expands the
-- array a second time below the JOIN and doubles that.
SELECT l.a FROM l_unstable_key AS l INNER JOIN r_unstable_key AS r ON l.a = toInt64(arrayJoin([r.x, r.x]))
WHERE l.a BETWEEN 5 AND 6 ORDER BY 1;

SELECT 'equality reversed';

SELECT l.a FROM l_unstable_key AS l INNER JOIN r_unstable_key AS r ON toInt64(arrayJoin([r.x, r.x])) = l.a
WHERE l.a BETWEEN 5 AND 6 ORDER BY 1;

SELECT 'unstable key on the left side';

SELECT r.x FROM l_unstable_key AS l INNER JOIN r_unstable_key AS r ON toInt64(arrayJoin([l.a, l.a])) = r.x
WHERE r.x BETWEEN 5 AND 6 ORDER BY 1;

SELECT 'left join';

SELECT l.a FROM l_unstable_key AS l LEFT JOIN r_unstable_key AS r ON l.a = toInt64(arrayJoin([r.x, r.x]))
WHERE l.a BETWEEN 5 AND 6 ORDER BY 1;

SELECT 'runtime filters on';

SELECT l.a FROM l_unstable_key AS l INNER JOIN r_unstable_key AS r ON l.a = toInt64(arrayJoin([r.x, r.x]))
WHERE l.a BETWEEN 5 AND 6 ORDER BY 1 SETTINGS enable_join_runtime_filters = 1;

SELECT 'runtime filters off';

SELECT l.a FROM l_unstable_key AS l INNER JOIN r_unstable_key AS r ON l.a = toInt64(arrayJoin([r.x, r.x]))
WHERE l.a BETWEEN 5 AND 6 ORDER BY 1 SETTINGS enable_join_runtime_filters = 0;

SELECT 'control: predicate on the unstable key itself';

-- The predicate already stands where the JOIN evaluates the key, so nothing is substituted and the fix must
-- not change this answer. Nothing is pushed below the JOIN here either: the key is an internal expression of
-- the JOIN, not one of its output columns, so no filter above the JOIN can name it.
SELECT l.a FROM l_unstable_key AS l INNER JOIN r_unstable_key AS r ON l.a = toInt64(arrayJoin([r.x, r.x]))
WHERE toInt64(arrayJoin([r.x, r.x])) BETWEEN 5 AND 6 ORDER BY 1;

SELECT 'control: both keys unstable';

SELECT l.a FROM l_unstable_key AS l
INNER JOIN r_unstable_key AS r ON toInt64(arrayJoin([l.a, l.a])) = toInt64(arrayJoin([r.x, r.x]))
WHERE l.a BETWEEN 5 AND 6 ORDER BY 1;

SELECT 'control: plain column key';

SELECT l.a FROM l_unstable_key AS l INNER JOIN r_unstable_key AS r ON l.a = r.x
WHERE l.a BETWEEN 5 AND 6 ORDER BY 1;

SELECT 'the key is not pushed below the join';

-- A recomputed `arrayJoin` under the right table's read is the wrong plan itself, independently of the rows.
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT l.a FROM l_unstable_key AS l INNER JOIN r_unstable_key AS r ON l.a = toInt64(arrayJoin([r.x, r.x]))
    WHERE l.a BETWEEN 5 AND 6
) WHERE explain ILIKE '%Filter column: toInt64(arrayJoin%';

-- A stateful key has no row oracle - `rowNumberInAllBlocks()` counts whatever rows reach it - so the plan is
-- the only statement available about it. It also carries no `arrayJoin`, which is why refusing the
-- substitution, rather than pruning a node, is what covers the class.
-- Both patterns match the substituted key at the HEAD of the filter, where a substitution puts it. A join
-- runtime filter names the opposite side's key too, in the middle of the other side's filter, and is a
-- single evaluation the JOIN binds by name rather than a second one.
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT l.a FROM l_unstable_key AS l
    INNER JOIN r_unstable_key AS r ON l.a = toInt64(rowNumberInAllBlocks()) + r.x * 0
    WHERE l.a BETWEEN 5 AND 6
) WHERE explain ILIKE '%Filter column: toInt64(rowNumberInAllBlocks%';

SELECT 'one array join action in the plan';

-- Counting the action lines the legacy renderer prints is independent of `__tableN` aliasing. Only the
-- renderer selected here prints them, so the assertion is vacuous without this pin. The JOIN's own
-- evaluation is the one that survives.
SET explain_query_plan_default = 'legacy';
SELECT countSubstrings(arrayStringConcat(groupArray(explain), char(10)), 'ARRAY JOIN ') FROM (
    EXPLAIN actions = 1
    SELECT l.a FROM l_unstable_key AS l INNER JOIN r_unstable_key AS r ON l.a = toInt64(arrayJoin([r.x, r.x]))
    WHERE l.a BETWEEN 5 AND 6
);
SET explain_query_plan_default = 'pretty';

DROP TABLE l_unstable_key;
DROP TABLE r_unstable_key;

SELECT 'a stable key still reaches both indexes';

DROP TABLE IF EXISTS lk_stable_key;
DROP TABLE IF EXISTS rk_stable_key;

CREATE TABLE lk_stable_key (a Int64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE rk_stable_key (x Int64) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO lk_stable_key SELECT number FROM numbers(100000);
INSERT INTO rk_stable_key SELECT number FROM numbers(100000);

-- The point of the substitution is the index condition it produces on the side the predicate was not
-- written against. The two settings pinned off are other producers of the same condition, so without
-- them the assertion cannot fail: one copies the directly pushed predicate across the same equi key.
SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT lk.a FROM lk_stable_key AS lk INNER JOIN rk_stable_key AS rk ON lk.a = rk.x WHERE lk.a = 5
    SETTINGS query_plan_propagate_predicate_across_join = 0, enable_join_runtime_filters = 0
) WHERE explain ILIKE '%Condition: (x in [5, 5])%';

SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT lk.a FROM lk_stable_key AS lk INNER JOIN rk_stable_key AS rk ON lk.a = rk.x WHERE lk.a = 5
    SETTINGS query_plan_propagate_predicate_across_join = 0, enable_join_runtime_filters = 0
) WHERE explain ILIKE '%Condition: (a in [5, 5])%';

SELECT 'a deterministic expression key keeps the substitution';

-- A stable expression key is still admitted, so the substitution reaches the opposite side's index. The
-- probes above join on plain columns, which pass the check without reaching its function test; only this
-- arm reds if the check starts refusing every expression key.
SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT lk.a FROM lk_stable_key AS lk INNER JOIN rk_stable_key AS rk ON lk.a = rk.x + 1 WHERE lk.a = 5
    SETTINGS query_plan_propagate_predicate_across_join = 0, enable_join_runtime_filters = 0
) WHERE explain ILIKE '%Condition: (plus(x, 1) in [5, 5])%';

SELECT 'a key observable beyond its value is not substituted';

-- `sleep` returns the same value for every row and is neither stateful nor non-deterministic, so its observable
-- side effects are the only thing that disqualifies it: a substituted key runs below the JOIN over that side's
-- whole read while the JOIN runs it again above, which a user reads back as extra `SleepFunctionCalls` and time.
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT lk.a FROM lk_stable_key AS lk INNER JOIN rk_stable_key AS rk ON lk.a = rk.x + sleep(0)
    WHERE lk.a = 5
    SETTINGS query_plan_propagate_predicate_across_join = 0, enable_join_runtime_filters = 0
) WHERE explain ILIKE '%Filter column: %sleep%';

-- `materialize` is the same shape, an expression that is not constant-folded either, and is still substituted,
-- so the pair cannot pass by refusing both.
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT lk.a FROM lk_stable_key AS lk INNER JOIN rk_stable_key AS rk ON lk.a = rk.x + materialize(0::Int64)
    WHERE lk.a = 5
    SETTINGS query_plan_propagate_predicate_across_join = 0, enable_join_runtime_filters = 0
) WHERE explain ILIKE '%Filter column: %materialize%';

DROP TABLE lk_stable_key;
DROP TABLE rk_stable_key;
