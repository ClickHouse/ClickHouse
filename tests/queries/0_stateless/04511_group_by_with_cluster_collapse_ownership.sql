-- Regression: `ClusterMergingTransform` folds aggregate states together in place, but
-- `ColumnAggregateFunction` state ownership cannot be shared per individual row (only
-- for the whole column). When many groups collapse into a single cluster (a large
-- distance), two rows could alias the same state pointer and tripped the assertion
-- "IAggregateFunction::merge called with the same source and destination state" (found
-- by the AST fuzzer). Taking unique ownership of every state up front fixes it; the
-- result of a full collapse must equal the ungrouped aggregate over all rows.

SET enable_analyzer = 1; -- `WITH CLUSTER` is implemented for the analyzer only
SET allow_experimental_group_by_with_cluster = 1;

-- 257 numeric groups, all within the distance -> one cluster. The merged `sum` state
-- must equal the sum over every row.
SELECT 'sum collapses to one cluster';
SELECT
(
    SELECT sum(s) FROM (SELECT sum(v) AS s FROM (SELECT number % 257 AS k, number AS v FROM numbers(1024)) GROUP BY k WITH CLUSTER 65535)
) = (
    SELECT sum(number) FROM numbers(1024)
);

-- Same, exercising the `-Distinct` combinator state merge (the aggregate from the
-- fuzzer report).
SELECT 'sumDistinct collapses to one cluster';
SELECT
(
    SELECT sum(s) FROM (SELECT sumDistinct(v) AS s FROM (SELECT number % 257 AS k, number AS v FROM numbers(1024)) GROUP BY k WITH CLUSTER 65535)
) = (
    SELECT sumDistinct(number) FROM numbers(1024)
);

-- Same for `count`.
SELECT 'count collapses to one cluster';
SELECT
(
    SELECT sum(c) FROM (SELECT count() AS c FROM (SELECT number % 257 AS k FROM numbers(1024)) GROUP BY k WITH CLUSTER 65535)
) = 1024;

-- An allocating aggregate (`groupArray`) must also merge correctly across the collapse.
SELECT 'groupArray length after collapse';
SELECT length(arr) FROM (
    SELECT groupArray(v) AS arr FROM (SELECT number % 257 AS k, number AS v FROM numbers(1024)) GROUP BY k WITH CLUSTER 65535
);
