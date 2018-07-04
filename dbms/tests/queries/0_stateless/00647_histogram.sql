select histogram(5)(number-10) from (select * from system.numbers limit 20);
select histogram(5)(number) from (select * from system.numbers limit 20);
select histogram(3)(sin(number)) from (select * from system.numbers limit 10);
select histogram(1)(sin(number-40)) from (select * from system.numbers limit 80);
