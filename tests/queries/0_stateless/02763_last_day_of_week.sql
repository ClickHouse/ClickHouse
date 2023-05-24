SELECT
    toDate('2018-12-25') + number AS x,
    toDateTime(x) AS x_t,
    toLastDayOfWeek(x) AS w0,
    toLastDayOfWeek(x_t) AS wt0,
    toLastDayOfWeek(x, 3) AS w3,
    toLastDayOfWeek(x_t, 3) AS wt3
FROM numbers(10);

