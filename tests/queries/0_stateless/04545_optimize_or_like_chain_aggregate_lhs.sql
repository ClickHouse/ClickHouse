-- Aggregate/window functions on the LHS of a LIKE chain must not trip the
-- optimize_or_like_chain non-determinism check (previously threw LOGICAL_ERROR
-- "Function node with name '...' is not resolved as ordinary function").
SET optimize_or_like_chain = 1;

SELECT toString(count()) LIKE '%1%' OR toString(count()) LIKE '%2%' FROM numbers(3);
SELECT toString(sum(number) OVER ()) LIKE '%1%' OR toString(sum(number) OVER ()) LIKE '%2%' FROM numbers(3);

-- Ordinary (deterministic) functions still collapse correctly.
SELECT toString(number + 1) LIKE '%1%' OR toString(number + 1) LIKE '%2%' FROM numbers(3);
