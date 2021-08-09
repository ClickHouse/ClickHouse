drop table if exists spark_bar_test;

CREATE TABLE spark_bar_test (`cnt` UInt64,`event_date` Date) ENGINE = MergeTree ORDER BY event_date SETTINGS index_granularity = 8192;

insert into spark_bar_test values(1,'2020-01-01'),(4,'2020-01-02'),(5,'2020-01-03'),(2,'2020-01-04'),(3,'2020-01-05'),(7,'2020-01-06'),(6,'2020-01-07'),(8,'2020-01-08'),(2,'2020-01-11');

select sparkbar(1)(event_date,cnt) from spark_bar_test;
select sparkbar(2)(event_date,cnt) from spark_bar_test;
select sparkbar(3)(event_date,cnt) from spark_bar_test;
select sparkbar(4)(event_date,cnt) from spark_bar_test;
select sparkbar(5)(event_date,cnt) from spark_bar_test;
select sparkbar(6)(event_date,cnt) from spark_bar_test;
select sparkbar(7)(event_date,cnt) from spark_bar_test;
select sparkbar(8)(event_date,cnt) from spark_bar_test;
select sparkbar(9)(event_date,cnt) from spark_bar_test;
select sparkbar(10)(event_date,cnt) from spark_bar_test;
select sparkbar(11)(event_date,cnt) from spark_bar_test;
