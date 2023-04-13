-- { echoOn }
with arrayJoin([0, 1, 2, 10]) as x select quantilesApprox(100, 0.5, 0.4, 0.1)(x);
with arrayJoin([0, 6, 7, 9, 10]) as x select quantileApprox(100, 0.5)(x);

select quantilesApprox(10000, 0.25, 0.5, 0.75, 0.0, 1.0, 0, 1)(number + 1) from numbers(1000);
select quantilesApprox(10000, 0.01, 0.1, 0.11)(number + 1) from numbers(10);

with number + 1 as col select quantilesApprox(10000, 0.25, 0.5, 0.75)(col), count(col), quantilesApprox(10000, 0.0, 1.0)(col), sum(col) from numbers(1000);

select quantilesApprox(1, 100/1000, 200/1000, 250/1000, 314/1000, 777/1000)(number + 1) from numbers(1000);
select quantilesApprox(10, 100/1000, 200/1000, 250/1000, 314/1000, 777/1000)(number + 1) from numbers(1000);
select quantilesApprox(100, 100/1000, 200/1000, 250/1000, 314/1000, 777/1000)(number + 1) from numbers(1000);
select quantilesApprox(1000, 100/1000, 200/1000, 250/1000, 314/1000, 777/1000)(number + 1) from numbers(1000);
select quantilesApprox(10000, 100/1000, 200/1000, 250/1000, 314/1000, 777/1000)(number + 1) from numbers(1000);


select quantileApprox()(number) from numbers(10); -- { serverError BAD_ARGUMENTS }
select quantileApprox(100)(number) from numbers(10);
select quantileApprox(100, 0.5)(number) from numbers(10);
select quantileApprox(100, 0.5, 0.75)(number) from numbers(10); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select quantileApprox('abc', 0.5)(number) from numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select quantileApprox(1.23, 0.5)(number) from numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select quantileApprox(-100, 0.5)(number) from numbers(10); -- { serverError BAD_ARGUMENTS }

select quantilesApprox()(number) from numbers(10); -- { serverError BAD_ARGUMENTS }
select quantilesApprox(100)(number) from numbers(10); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select quantilesApprox(100, 0.5)(number) from numbers(10);
select quantilesApprox('abc', 0.5, 0.75)(number) from numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select quantilesApprox(1.23, 0.5, 0.75)(number) from numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select quantilesApprox(-100, 0.5, 0.75)(number) from numbers(10); -- { serverError BAD_ARGUMENTS }
-- { echoOff }
