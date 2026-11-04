set enable_analyzer = 1;

-- { echoOn }
with arrayJoin([0, 1, 2, 10]) as x select quantilesGK(100, 0.5, 0.4, 0.1)(x);
with arrayJoin([0, 6, 7, 9, 10]) as x select quantileGK(100, 0.5)(x);

select quantilesGK(10000, 0.25, 0.5, 0.75, 0.0, 1.0, 0, 1)(number + 1) from numbers(1000);
select quantilesGK(10000, 0.01, 0.1, 0.11)(number + 1) from numbers(10);

with number + 1 as col select quantilesGK(10000, 0.25, 0.5, 0.75)(col), count(col), quantilesGK(10000, 0.0, 1.0)(col), sum(col) from numbers(1000);

select quantilesGK(1, 100/1000, 200/1000, 250/1000, 314/1000, 777/1000)(number + 1) from numbers(1000);
select quantilesGK(10, 100/1000, 200/1000, 250/1000, 314/1000, 777/1000)(number + 1) from numbers(1000);
select quantilesGK(100, 100/1000, 200/1000, 250/1000, 314/1000, 777/1000)(number + 1) from numbers(1000);
select quantilesGK(1000, 100/1000, 200/1000, 250/1000, 314/1000, 777/1000)(number + 1) from numbers(1000);
select quantilesGK(10000, 100/1000, 200/1000, 250/1000, 314/1000, 777/1000)(number + 1) from numbers(1000);

SELECT quantileGKMerge(100, 0.5)(x)
FROM
(
    SELECT quantileGKState(100, 0.5)(number + 1) AS x
    FROM numbers(49999)
);

SELECT quantilesGKMerge(100, 0.5, 0.9, 0.99)(x)
FROM
(
    SELECT quantilesGKState(100, 0.5, 0.9, 0.99)(number + 1) AS x
    FROM numbers(49999)
);

select medianGK()(number) from numbers(10) SETTINGS enable_analyzer = 0; -- { serverError BAD_ARGUMENTS }
select medianGK()(number) from numbers(10) SETTINGS enable_analyzer = 1; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

select quantileGK()(number) from numbers(10) SETTINGS enable_analyzer = 0; -- { serverError BAD_ARGUMENTS }
select quantileGK()(number) from numbers(10) SETTINGS enable_analyzer = 1; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

select medianGK(100)(number) from numbers(10);
select quantileGK(100)(number) from numbers(10);
select quantileGK(100, 0.5)(number) from numbers(10);
select quantileGK(100, 0.5, 0.75)(number) from numbers(10); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select quantileGK('abc', 0.5)(number) from numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select quantileGK(1.23, 0.5)(number) from numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select quantileGK(-100, 0.5)(number) from numbers(10); -- { serverError BAD_ARGUMENTS }

select quantilesGK()(number) from numbers(10) SETTINGS enable_analyzer = 0; -- { serverError BAD_ARGUMENTS }
select quantilesGK()(number) from numbers(10) SETTINGS enable_analyzer = 1; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

select quantilesGK(100)(number) from numbers(10); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select quantilesGK(100, 0.5)(number) from numbers(10);
select quantilesGK('abc', 0.5, 0.75)(number) from numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select quantilesGK(1.23, 0.5, 0.75)(number) from numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select quantilesGK(-100, 0.5, 0.75)(number) from numbers(10); -- { serverError BAD_ARGUMENTS }
-- { echoOff }
