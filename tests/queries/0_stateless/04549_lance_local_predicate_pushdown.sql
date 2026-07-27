DROP TABLE IF EXISTS lance_local_predicate_pushdown;

SET session_timezone = 'UTC';

CREATE TABLE lance_local_predicate_pushdown
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/pushdown.lance');

SELECT id, name FROM lance_local_predicate_pushdown WHERE id = 2;
SELECT id FROM lance_local_predicate_pushdown WHERE id > 1 AND score < 4 ORDER BY id;
SELECT id FROM lance_local_predicate_pushdown WHERE id = 1 OR id = 3 ORDER BY id;
SELECT id FROM lance_local_predicate_pushdown WHERE id < 2 OR score IS NULL ORDER BY id;
SELECT id FROM lance_local_predicate_pushdown WHERE id IN (1, 3, 5) ORDER BY id;
SELECT id FROM lance_local_predicate_pushdown WHERE name IN ('a', 'quote''d') ORDER BY id;
SELECT id FROM lance_local_predicate_pushdown WHERE score IS NULL ORDER BY id;
SELECT id FROM lance_local_predicate_pushdown WHERE score IS NOT NULL ORDER BY id;
SELECT count() FROM lance_local_predicate_pushdown WHERE score = CAST(NULL, 'Nullable(Float64)');
SELECT id FROM lance_local_predicate_pushdown WHERE event_date = toDate('2024-01-02') ORDER BY id;
SELECT id FROM lance_local_predicate_pushdown WHERE event_time >= toDateTime64('2024-01-02 03:04:05.123', 3) ORDER BY id;
SELECT id FROM lance_local_predicate_pushdown WHERE `odd``name` = 30;
SELECT id FROM lance_local_predicate_pushdown WHERE lower(name) = 'x' ORDER BY id;
SELECT id FROM lance_local_predicate_pushdown WHERE id + 1 = 3;
SELECT id FROM lance_local_predicate_pushdown WHERE score BETWEEN 1 AND 3 ORDER BY id;
-- Partial AND: comparison is pushable, lower(name) is not. Residual filter keeps results correct.
SELECT id, name FROM lance_local_predicate_pushdown WHERE id = 2 AND lower(name) = 'b';
SELECT id FROM lance_local_predicate_pushdown WHERE id = 2 AND lower(name) = 'nope';
SELECT id FROM lance_local_predicate_pushdown WHERE id IN (4, 7) AND lower(name) = 'x' ORDER BY id;
-- OR atom must translate fully; residual still evaluates lower.
SELECT id FROM lance_local_predicate_pushdown WHERE (id = 1 OR id = 3) AND lower(name) != 'zzz' ORDER BY id;

DROP TABLE lance_local_predicate_pushdown;
