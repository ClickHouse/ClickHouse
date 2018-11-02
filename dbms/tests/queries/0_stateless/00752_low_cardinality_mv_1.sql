SET allow_experimental_low_cardinality_type = 1;

drop table if exists test.lc;
drop table if exists test.lc_mv;

create table test.lc (str StringWithDictionary) engine = MergeTree order by tuple();

insert into test.lc values ('a'), ('bbb'), ('ab'), ('accccc'), ('baasddas'), ('bcde');

CREATE MATERIALIZED VIEW test.lc_mv ENGINE = AggregatingMergeTree() ORDER BY tuple() populate AS SELECT substring(str, 1, 1) as letter, min(length(str)) AS min_len, max(length(str)) AS max_len FROM test.lc GROUP BY substring(str, 1, 1);

insert into test.lc values ('a'), ('bbb'), ('ab'), ('accccc'), ('baasddas'), ('bcde');
select * from test.lc_mv order by letter;

drop table if exists test.lc;
drop table if exists test.lc_mv;

