select histogram(5)(number-10) from (select * from system.numbers limit 20);
select histogram(5)(number) from (select * from system.numbers limit 20);

WITH arrayJoin(histogram(3)(sin(number))) AS res select round(res.1, 2), round(res.2, 2), round(res.3, 2) from (select * from system.numbers limit 10);
WITH arrayJoin(histogram(1)(sin(number-40))) AS res SELECT round(res.1, 2), round(res.2, 2), round(res.3, 2) from (select * from system.numbers limit 80);

SELECT histogram(10)(-2);

select histogramIf(3)(number, number > 11) from (select * from system.numbers limit 10);
