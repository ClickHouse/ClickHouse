-- The `parallel_full_sorting_merge` join algorithm shards a full sorting merge join by the hash of the
-- join keys and runs one merge join per shard. It must return the same results as the `hash` algorithm.

DROP TABLE IF EXISTS pfsmj_left;
DROP TABLE IF EXISTS pfsmj_right;

CREATE TABLE pfsmj_left (id UInt64, a UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE pfsmj_right (id UInt64, b UInt64) ENGINE = MergeTree ORDER BY id;

-- Insert as several parts so the read produces multiple streams and the join is actually sharded.
INSERT INTO pfsmj_left SELECT number, number FROM numbers(0, 30000);
INSERT INTO pfsmj_left SELECT number, number FROM numbers(30000, 30000);
INSERT INTO pfsmj_left SELECT number, number FROM numbers(60000, 30000);
-- Repeated keys on the right exercise many-to-many matches within a shard.
INSERT INTO pfsmj_right SELECT number % 40000, number * 2 FROM numbers(90000);

-- Each check computes an aggregate over the join twice - once with `parallel_full_sorting_merge` and once
-- with `hash` - and prints whether they are equal. All must print 1.

SELECT 'inner',
    (SELECT (sum(l.a + r.b), count()) FROM pfsmj_left AS l INNER JOIN pfsmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge')
  = (SELECT (sum(l.a + r.b), count()) FROM pfsmj_left AS l INNER JOIN pfsmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'left',
    (SELECT (sum(l.a), count()) FROM pfsmj_left AS l LEFT JOIN pfsmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge')
  = (SELECT (sum(l.a), count()) FROM pfsmj_left AS l LEFT JOIN pfsmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'right',
    (SELECT (sum(r.b), count()) FROM pfsmj_left AS l RIGHT JOIN pfsmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge')
  = (SELECT (sum(r.b), count()) FROM pfsmj_left AS l RIGHT JOIN pfsmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'full',
    (SELECT (sum(l.a), sum(r.b), count()) FROM pfsmj_left AS l FULL JOIN pfsmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge')
  = (SELECT (sum(l.a), sum(r.b), count()) FROM pfsmj_left AS l FULL JOIN pfsmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

-- With join_use_nulls the non-matching side is filled with NULLs; the results must still match.
SELECT 'left_use_nulls',
    (SELECT (sum(l.a), sum(r.b), count()) FROM pfsmj_left AS l LEFT JOIN pfsmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge', join_use_nulls = 1)
  = (SELECT (sum(l.a), sum(r.b), count()) FROM pfsmj_left AS l LEFT JOIN pfsmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash', join_use_nulls = 1);

-- Row-level check (not just aggregates): the two result sets must be identical.
SELECT 'rows_identical', count() = 0
FROM
(
    (SELECT l.id, l.a, r.b FROM pfsmj_left AS l INNER JOIN pfsmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge')
    EXCEPT
    (SELECT l.id, l.a, r.b FROM pfsmj_left AS l INNER JOIN pfsmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash')
);

DROP TABLE pfsmj_left;
DROP TABLE pfsmj_right;
