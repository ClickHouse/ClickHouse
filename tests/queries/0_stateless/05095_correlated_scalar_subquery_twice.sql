-- A correlated scalar subquery is decorrelated by the planner, so the analyzer leaves it alone where
-- it resolves it. The same subquery written twice in one expression resolves the second occurrence
-- from the analyzer's cache, and that path evaluated it as an ordinary scalar - rejecting the query
-- with "Cannot evaluate correlated scalar subquery".

SET allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS t_correlated_twice;
CREATE TABLE t_correlated_twice (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_correlated_twice SELECT number FROM numbers(5);

SELECT 'one occurrence';
SELECT o.id, (SELECT count() FROM t_correlated_twice AS i WHERE i.id = o.id) FROM t_correlated_twice AS o ORDER BY o.id;

SELECT 'both branches of a constant condition';
SELECT o.id, if(1 = 1, (SELECT count() FROM t_correlated_twice AS i WHERE i.id = o.id),
                       (SELECT count() FROM t_correlated_twice AS i WHERE i.id = o.id))
FROM t_correlated_twice AS o ORDER BY o.id;
SELECT o.id, if(1 = 0, (SELECT count() FROM t_correlated_twice AS i WHERE i.id = o.id),
                       (SELECT count() FROM t_correlated_twice AS i WHERE i.id = o.id))
FROM t_correlated_twice AS o ORDER BY o.id;
SELECT o.id, multiIf(1 = 1, (SELECT count() FROM t_correlated_twice AS i WHERE i.id = o.id),
                            (SELECT count() FROM t_correlated_twice AS i WHERE i.id = o.id))
FROM t_correlated_twice AS o ORDER BY o.id;

SELECT 'a non-constant condition, which always worked';
SELECT o.id, if(o.id >= 0, (SELECT count() FROM t_correlated_twice AS i WHERE i.id = o.id),
                           (SELECT count() FROM t_correlated_twice AS i WHERE i.id = o.id))
FROM t_correlated_twice AS o ORDER BY o.id;

SELECT 'the same subquery twice in other shapes';
SELECT o.id, (SELECT count() FROM t_correlated_twice AS i WHERE i.id = o.id)
           + (SELECT count() FROM t_correlated_twice AS i WHERE i.id = o.id)
FROM t_correlated_twice AS o ORDER BY o.id;
SELECT o.id, greatest((SELECT count() FROM t_correlated_twice AS i WHERE i.id = o.id),
                      (SELECT count() FROM t_correlated_twice AS i WHERE i.id = o.id))
FROM t_correlated_twice AS o ORDER BY o.id;

DROP TABLE t_correlated_twice;
