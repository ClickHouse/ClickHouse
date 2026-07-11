select transform(2, [1,2], [9,1], materialize(null));
select transform(2, [1,2], [9,1], materialize(7));
select transform(2, [1,2], [9,1], null);
select transform(2, [1,2], [9,1], 7);
select transform(1, [1,2], [9,1], null);
select transform(1, [1,2], [9,1], 7);
select transform(5, [1,2], [9,1], null);
select transform(5, [1,2], [9,1], 7);
select transform(2, [1,2], [9,1]);
select transform(1, [1,2], [9,1]);
select transform(7, [1,2], [9,1]);

select transform(2, [1,2], ['a','b'], materialize(null));
select transform(2, [1,2], ['a','b'], materialize('c'));
select transform(2, [1,2], ['a','b'], null);
select transform(2, [1,2], ['a','b'], 'c');
select transform(1, [1,2], ['a','b'], null);
select transform(1, [1,2], ['a','b'], 'c');
select transform(5, [1,2], ['a','b'], null);
select transform(5, [1,2], ['a','b'], 'c');

select 'sep1';
SELECT transform(number, [2], [toDecimal32(1, 1)], materialize(80000)) as x FROM numbers(2);
select 'sep2';
SELECT transform(number, [2], [toDecimal32(1, 1)], 80000) as x FROM numbers(2);
select 'sep3';
SELECT transform(toDecimal32(2, 1), [toDecimal32(2, 1)], [1]);
select 'sep4';
SELECT transform(8000, [1], [toDecimal32(2, 1)]);
select 'sep5';
SELECT transform(toDecimal32(8000,0), [1], [toDecimal32(2, 1)]);
select 'sep6';
SELECT transform(-9223372036854775807, [-1], [toDecimal32(1024, 3)]) FROM system.numbers LIMIT 7; -- { serverError BAD_ARGUMENTS }
SELECT [NULL, NULL, NULL, NULL], transform(number, [2147483648], [toDecimal32(1, 2)]) AS x FROM numbers(257) WHERE materialize(10); -- { serverError BAD_ARGUMENTS }
SELECT transform(-2147483649, [1], [toDecimal32(1, 2)]) GROUP BY [1] WITH TOTALS; -- { serverError BAD_ARGUMENTS }

SELECT 'issue #53187';
SELECT
    CAST(number, 'String') AS v2,
    caseWithExpression('x', 'y', 0, cond2) AS cond1,
    toNullable('0' = v2) AS cond2
FROM numbers(2);
SELECT '-';
SELECT
    CAST(number, 'String') AS v2,
    caseWithExpression('x', 'y', 0, cond2) AS cond1,
    toNullable('1' = v2) AS cond2
FROM numbers(2);
