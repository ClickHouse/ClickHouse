create table test_table ( `date` Date, `__sign` Int8, `from` Float64, `to` Float64 ) ENGINE = CollapsingMergeTree(__sign) PARTITION BY toYYYYMM(date) ORDER BY (date) SETTINGS index_granularity = 8192;
create VIEW test_view AS WITH cte AS (SELECT date, __sign, "from", "to" FROM test_table FINAL) SELECT date, __sign, "from", "to" FROM cte;
show create table test_view;
drop table test_view;
drop table test_table;
