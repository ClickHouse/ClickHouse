DROP TABLE IF EXISTS t1;

CREATE TABLE t1
(
    date Date, 
    s1 String,
    s2 String
) ENGINE = MergeTree() PARTITION BY toYYYYMMDD(date) ORDER BY (date, s1)
SETTINGS index_granularity = 8192;

insert into t1 (date, s1,s2) values(today()-1,'aaa','bbb');
alter table t1 add column s3 String DEFAULT concat(s2,'_',s1);
insert into t1 (date, s1,s2) values(today(),'aaa2','bbb2');
select ignore(date), s3 from t1 where  s2='bbb';

DROP TABLE t1;
