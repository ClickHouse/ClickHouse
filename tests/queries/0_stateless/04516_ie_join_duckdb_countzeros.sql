-- Tags: no-old-analyzer

-- A 100k self-join of one-second states where every row matches exactly itself,
-- exercising the boundary of the word-level scan over the match bit array.
-- With an additional equality on `k` the join has a hash key and is not executed
-- by IEJoin, so both variants are checked.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';

DROP TABLE IF EXISTS states;

CREATE TABLE states ENGINE = MergeTree ORDER BY tuple() AS
SELECT intDiv(number, 100) AS k,
       toDateTime('2024-01-01 00:00:00', 'UTC') + toIntervalSecond(number) AS b,
       b + toIntervalSecond(1) AS e
FROM numbers(100000);

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM states lhs JOIN states rhs ON lhs.b < rhs.e AND rhs.b < lhs.e) WHERE explain LIKE '%IEJoin%';
SELECT count() FROM states lhs JOIN states rhs ON lhs.b < rhs.e AND rhs.b < lhs.e;

SELECT count() FROM (EXPLAIN actions = 1 SELECT count() FROM states lhs JOIN states rhs ON lhs.b < rhs.e AND rhs.b < lhs.e AND lhs.k = rhs.k) WHERE explain LIKE '%IEJoin%';
SELECT count() FROM states lhs JOIN states rhs ON lhs.b < rhs.e AND rhs.b < lhs.e AND lhs.k = rhs.k;

DROP TABLE states;
