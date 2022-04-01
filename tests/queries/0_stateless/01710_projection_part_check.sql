-- Tags: no-s3-storage
drop table if exists tp;

create table tp (x Int32, y Int32, projection p (select x, y order by x)) engine = MergeTree order by y settings min_rows_for_compact_part = 2, min_rows_for_wide_part = 4, min_bytes_for_compact_part = 16, min_bytes_for_wide_part = 32;

insert into tp select number, number from numbers(3);
insert into tp select number, number from numbers(5);

check table tp settings check_query_single_value_result=0;

drop table if exists tp;

CREATE TABLE tp (`p` Date, `k` UInt64, `v1` UInt64, `v2` Int64, PROJECTION p1 ( SELECT p, sum(k), sum(v1), sum(v2) GROUP BY p) ) ENGINE = MergeTree PARTITION BY toYYYYMM(p) ORDER BY k SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO tp (p, k, v1, v2) VALUES ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);

CHECK TABLE tp settings check_query_single_value_result=0;

DROP TABLE if exists tp;
