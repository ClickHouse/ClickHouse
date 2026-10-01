-- Tags: no-old-analyzer

-- Band join `a.t >= b.t_lo AND a.t <= b.t_hi` with `a.t = 0..N-1` and `b = [k, k+5]`
-- for `k = 0..N-1` must produce exactly `6 * N - 15` pairs.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT count()
    FROM (SELECT number AS t FROM numbers(10)) a
    JOIN (SELECT number AS t_lo, number + 5 AS t_hi FROM numbers(10)) b
    ON a.t >= b.t_lo AND a.t <= b.t_hi
) WHERE explain LIKE '%IEJoin%';

SELECT count()
FROM (SELECT number AS t FROM numbers(10)) a
JOIN (SELECT number AS t_lo, number + 5 AS t_hi FROM numbers(10)) b
ON a.t >= b.t_lo AND a.t <= b.t_hi;

SELECT count()
FROM (SELECT number AS t FROM numbers(1000)) a
JOIN (SELECT number AS t_lo, number + 5 AS t_hi FROM numbers(1000)) b
ON a.t >= b.t_lo AND a.t <= b.t_hi;

SELECT count()
FROM (SELECT number AS t FROM numbers(100000)) a
JOIN (SELECT number AS t_lo, number + 5 AS t_hi FROM numbers(100000)) b
ON a.t >= b.t_lo AND a.t <= b.t_hi;
