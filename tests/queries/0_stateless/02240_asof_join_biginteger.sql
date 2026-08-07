select * from (select 0 as k, toInt128('18446744073709551617') as v) t1 asof join (select 0 as k, toInt128('18446744073709551616') as v) t2 using(k, v);
select * from (select 0 as k, toInt256('340282366920938463463374607431768211457') as v) t1 asof join (select 0 as k, toInt256('340282366920938463463374607431768211456') as v) t2 using(k, v);

select * from (select 0 as k, toUInt128('18446744073709551617') as v) t1 asof join (select 0 as k, toUInt128('18446744073709551616') as v) t2 using(k, v);
select * from (select 0 as k, toUInt256('340282366920938463463374607431768211457') as v) t1 asof join (select 0 as k, toUInt256('340282366920938463463374607431768211456') as v) t2 using(k, v);

SET join_algorithm = 'full_sorting_merge';

select * from (select 0 as k, toInt128('18446744073709551617') as v) t1 asof join (select 0 as k, toInt128('18446744073709551616') as v) t2 using(k, v);
select * from (select 0 as k, toInt256('340282366920938463463374607431768211457') as v) t1 asof join (select 0 as k, toInt256('340282366920938463463374607431768211456') as v) t2 using(k, v);

select * from (select 0 as k, toUInt128('18446744073709551617') as v) t1 asof join (select 0 as k, toUInt128('18446744073709551616') as v) t2 using(k, v);
select * from (select 0 as k, toUInt256('340282366920938463463374607431768211457') as v) t1 asof join (select 0 as k, toUInt256('340282366920938463463374607431768211456') as v) t2 using(k, v);
