drop table if exists lc_mv_00752;
drop table if exists lc_00752;

create table lc_00752 (str StringWithDictionary) engine = MergeTree order by tuple();

insert into lc_00752 values ('a'), ('bbb'), ('ab'), ('accccc'), ('baasddas'), ('bcde');

CREATE MATERIALIZED VIEW lc_mv_00752 ENGINE = AggregatingMergeTree() ORDER BY tuple() populate AS SELECT substring(str, 1, 1) as letter, min(length(str)) AS min_len, max(length(str)) AS max_len FROM lc_00752 GROUP BY substring(str, 1, 1);

insert into lc_00752 values ('a'), ('bbb'), ('ab'), ('accccc'), ('baasddas'), ('bcde');
select * from lc_mv_00752 order by letter;

drop table lc_mv_00752;
drop table lc_00752;
