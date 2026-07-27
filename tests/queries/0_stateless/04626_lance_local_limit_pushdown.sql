DROP TABLE IF EXISTS lance_local_limit_pushdown;

SET session_timezone = 'UTC';

CREATE TABLE lance_local_limit_pushdown
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/pushdown.lance');

-- No WHERE: LIMIT may be pushed into the Lance scanner. Result size is exact.
SELECT count() FROM (SELECT id FROM lance_local_limit_pushdown LIMIT 1);
SELECT count() FROM (SELECT id FROM lance_local_limit_pushdown LIMIT 3);
SELECT count() FROM (SELECT id FROM lance_local_limit_pushdown LIMIT 100);

-- Complete predicate + LIMIT: safe to push limit with the filter.
SELECT id FROM lance_local_limit_pushdown WHERE id >= 1 ORDER BY id LIMIT 2;
SELECT id FROM lance_local_limit_pushdown WHERE id IN (1, 3, 5, 7) ORDER BY id LIMIT 2;

-- Incomplete predicate + LIMIT: limit must not be pushed (residual can drop rows).
-- Correctness: still return the right number of matching rows.
SELECT id FROM lance_local_limit_pushdown WHERE id IN (4, 7) AND lower(name) = 'x' ORDER BY id LIMIT 10;
SELECT count() FROM lance_local_limit_pushdown WHERE id > 0 AND lower(name) = 'x';

-- OFFSET: plan passes limit+offset as source upper bound; CH LimitStep applies offset.
SELECT id FROM lance_local_limit_pushdown ORDER BY id LIMIT 1 OFFSET 2;

DROP TABLE lance_local_limit_pushdown;
