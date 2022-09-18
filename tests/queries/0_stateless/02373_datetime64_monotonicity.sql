drop table if exists dt64_monot_test sync;

create table dt64_monot_test(date_time DateTime64(3), id String)
    Engine=MergeTree
        partition by toDate(date_time)
        order by date_time;

insert into dt64_monot_test select toDateTime64('2020-01-01 00:00:00.000',3)+number , '' from numbers(10000000);

SELECT count() FROM dt64_monot_test;
SELECT count() FROM dt64_monot_test WHERE date_time >= toDateTime64('2020-01-20 00:00:00.000',3);
SELECT count() FROM dt64_monot_test WHERE toDateTime64(date_time,3) >= toDateTime64('2020-01-20 00:00:00.000',3);

drop table if exists dt64_monot_test sync;