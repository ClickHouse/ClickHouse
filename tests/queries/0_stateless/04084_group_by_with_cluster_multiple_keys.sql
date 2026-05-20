-- Multiple keys with `WITH CLUSTER` are not supported and must be rejected.
SELECT a, b, count() FROM VALUES('a UInt64, b UInt64', (1, 1), (2, 2))
GROUP BY a WITH CLUSTER 1, b WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }
