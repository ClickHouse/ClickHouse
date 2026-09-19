-- Tags: no-fasttest
-- Tag no-fasttest: COLLATE needs ICU, which the fast test build does not have.

-- Peers are decided by comparing the ORDER BY values, and that comparison does not consult a
-- collator, here or anywhere else in the window transform. Rather than take the wrong rows out of
-- the frame, the exclusions that are defined in terms of peers refuse a collated window order.
SELECT sum(v) OVER (ORDER BY s COLLATE 'en' RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) FROM (SELECT 'a' AS s, 1 AS v); -- { serverError NOT_IMPLEMENTED }
SELECT sum(v) OVER (ORDER BY s COLLATE 'en' RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) FROM (SELECT 'a' AS s, 1 AS v); -- { serverError NOT_IMPLEMENTED }

-- CURRENT ROW takes out one row rather than a peer group, so it does not depend on the comparison.
SELECT sum(v) OVER (ORDER BY s COLLATE 'en' ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM (SELECT 'a' AS s, 1 AS v);

-- A function that never reads the frame cannot be given the wrong rows, so a window carrying only
-- one of those takes the clause whatever the order is collated with.
SELECT row_number() OVER (ORDER BY s COLLATE 'en' RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) FROM (SELECT arrayJoin(['a', 'b']) AS s) ORDER BY ALL;

-- Without a collator the peer exclusions work as they do everywhere else.
SELECT sum(v) OVER (ORDER BY s RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) FROM (SELECT 'a' AS s, 1 AS v);
