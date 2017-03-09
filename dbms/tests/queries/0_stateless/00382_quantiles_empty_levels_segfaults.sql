select quantiles(number) as q from (select * from system.numbers LIMIT 1000);
select quantilesExact(number) as q from (select * from system.numbers LIMIT 1000);
select quantilesExactWeighted(number, number) as q from (select * from system.numbers LIMIT 1000);
select quantilesDeterministic(number, 10000000) as q from (select * from system.numbers LIMIT 1000);
select quantilesTiming(number) as q from (select * from system.numbers LIMIT 1000);
select quantilesTimingWeighted(number, number) as q from (select * from system.numbers LIMIT 1000);
select quantilesTDigest(number) as q from (select * from system.numbers LIMIT 1000);
select quantilesTDigestWeighted(number, number) as q from (select * from system.numbers LIMIT 1000);
