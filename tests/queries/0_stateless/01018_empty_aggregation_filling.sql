SELECT '--- Int Empty ---';

SELECT arrayReduce('avgOrDefault', arrayPopBack([1]));
SELECT arrayReduce('avgOrNull', arrayPopBack([1]));
SELECT arrayReduce('stddevSampOrDefault', arrayPopBack([1]));
SELECT arrayReduce('stddevSampOrNull', arrayPopBack([1]));
SELECT arrayReduce('maxOrDefault', arrayPopBack([1]));
SELECT arrayReduce('maxOrNull', arrayPopBack([1]));

SELECT avgOrDefaultIf(x, x > 1) FROM (SELECT 1 AS x);
SELECT avgOrNullIf(x, x > 1) FROM (SELECT 1 AS x);
SELECT stddevSampOrDefaultIf(x, x > 1) FROM (SELECT 1 AS x);
SELECT stddevSampOrNullIf(x, x > 1) FROM (SELECT 1 AS x);
SELECT maxOrDefaultIf(x, x > 1) FROM (SELECT 1 AS x);
SELECT maxOrNullIf(x, x > 1) FROM (SELECT 1 AS x);

SELECT avgOrDefaultIfMerge(state) FROM (SELECT avgOrDefaultIfState(x, x > 1) AS state FROM (SELECT 1 AS x));
SELECT avgOrNullIfMerge(state) FROM (SELECT avgOrNullIfState(x, x > 1) AS state FROM (SELECT 1 AS x));
SELECT stddevSampOrDefaultIfMerge(state) FROM (SELECT stddevSampOrDefaultIfState(x, x > 1) AS state FROM (SELECT 1 AS x));
SELECT stddevSampOrNullIfMerge(state) FROM (SELECT stddevSampOrNullIfState(x, x > 1) AS state FROM (SELECT 1 AS x));
SELECT maxOrDefaultIfMerge(state) FROM (SELECT maxOrDefaultIfState(x, x > 1) AS state FROM (SELECT 1 AS x));
SELECT maxOrNullIfMerge(state) FROM (SELECT maxOrNullIfState(x, x > 1) AS state FROM (SELECT 1 AS x));

SELECT '--- Int Non-empty ---';

SELECT arrayReduce('avgOrDefault', [1]);
SELECT arrayReduce('avgOrNull', [1]);
SELECT arrayReduce('stddevSampOrDefault', [1]);
SELECT arrayReduce('stddevSampOrNull', [1]);
SELECT arrayReduce('maxOrDefault', [1]);
SELECT arrayReduce('maxOrNull', [1]);

SELECT avgOrDefaultIf(x, x > 0) FROM (SELECT 1 AS x);
SELECT avgOrNullIf(x, x > 0) FROM (SELECT 1 AS x);
SELECT stddevSampOrDefaultIf(x, x > 0) FROM (SELECT 1 AS x);
SELECT stddevSampOrNullIf(x, x > 0) FROM (SELECT 1 AS x);
SELECT maxOrDefaultIf(x, x > 0) FROM (SELECT 1 AS x);
SELECT maxOrNullIf(x, x > 0) FROM (SELECT 1 AS x);

SELECT avgOrDefaultIfMerge(state) FROM (SELECT avgOrDefaultIfState(x, x > 0) AS state FROM (SELECT 1 AS x));
SELECT avgOrNullIfMerge(state) FROM (SELECT avgOrNullIfState(x, x > 0) AS state FROM (SELECT 1 AS x));
SELECT stddevSampOrDefaultIfMerge(state) FROM (SELECT stddevSampOrDefaultIfState(x, x > 0) AS state FROM (SELECT 1 AS x));
SELECT stddevSampOrNullIfMerge(state) FROM (SELECT stddevSampOrNullIfState(x, x > 0) AS state FROM (SELECT 1 AS x));
SELECT maxOrDefaultIfMerge(state) FROM (SELECT maxOrDefaultIfState(x, x > 0) AS state FROM (SELECT 1 AS x));
SELECT maxOrNullIfMerge(state) FROM (SELECT maxOrNullIfState(x, x > 0) AS state FROM (SELECT 1 AS x));

SELECT '--- Other Types Empty ---';

SELECT arrayReduce('maxOrDefault', arrayPopBack(['hello']));
SELECT arrayReduce('maxOrNull', arrayPopBack(['hello']));

SELECT arrayReduce('maxOrDefault', arrayPopBack(arrayPopBack([toDateTime('2011-04-05 14:19:19'), null])));
SELECT arrayReduce('maxOrNull', arrayPopBack(arrayPopBack([toDateTime('2011-04-05 14:19:19'), null])));

SELECT arrayReduce('avgOrDefault', arrayPopBack([toDecimal128(-123.45, 2)]));
SELECT arrayReduce('avgOrNull', arrayPopBack([toDecimal128(-123.45, 2)]));
SELECT arrayReduce('stddevSampOrDefault', arrayPopBack([toDecimal128(-123.45, 2)]));
SELECT arrayReduce('stddevSampOrNull', arrayPopBack([toDecimal128(-123.45, 2)]));
SELECT arrayReduce('maxOrDefault', arrayPopBack([toDecimal128(-123.45, 2)]));
SELECT arrayReduce('maxOrNull', arrayPopBack([toDecimal128(-123.45, 2)]));

SELECT '--- Other Types Non-empty ---';

SELECT arrayReduce('maxOrDefault', ['hello']);
SELECT arrayReduce('maxOrNull', ['hello']);

SELECT arrayReduce('maxOrDefault', [toDateTime('2011-04-05 14:19:19'), null]);
SELECT arrayReduce('maxOrNull', [toDateTime('2011-04-05 14:19:19'), null]);

SELECT arrayReduce('avgOrDefault', [toDecimal128(-123.45, 2)]);
SELECT arrayReduce('avgOrNull', [toDecimal128(-123.45, 2)]);
SELECT arrayReduce('stddevSampOrDefault', [toDecimal128(-123.45, 2)]);
SELECT arrayReduce('stddevSampOrNull', [toDecimal128(-123.45, 2)]);
SELECT arrayReduce('maxOrDefault', [toDecimal128(-123.45, 2)]);
SELECT arrayReduce('maxOrNull', [toDecimal128(-123.45, 2)]);
