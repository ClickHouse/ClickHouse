-- types
select argMin(x.1, x.2), argMax(x.1, x.2) from (select (number, number + 1) as x from numbers(10));
select argMin(x.1, x.2), argMax(x.1, x.2) from (select (toString(number), toInt32(number) + 1) as x from numbers(10));
select argMin(x.1, x.2), argMax(x.1, x.2) from (select (toDate(number, 'UTC'), toDateTime(number, 'UTC') + 1) as x from numbers(10));
select argMin(x.1, x.2), argMax(x.1, x.2) from (select (toDecimal32(number, 2), toDecimal64(number, 2) + 1) as x from numbers(10));

-- array
SELECT
    argMinArray(id, num),
    argMaxArray(id, num)
FROM
(
    SELECT
        arrayJoin([[10, 4, 3], [7, 5, 6], [8, 8, 2]]) AS num,
        arrayJoin([[1, 2, 4]]) AS id
);
