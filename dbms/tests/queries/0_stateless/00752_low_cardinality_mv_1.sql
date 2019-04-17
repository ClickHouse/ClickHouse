drop table if exists lc;
drop table if exists lc_mv;

create table lc (str StringWithDictionary) engine = MergeTree order by tuple();

insert into lc values ('a'), ('bbb'), ('ab'), ('accccc'), ('baasddas'), ('bcde');

CREATE MATERIALIZED VIEW lc_mv ENGINE = AggregatingMergeTree() ORDER BY tuple() populate AS SELECT substring(str, 1, 1) as letter, min(length(str)) AS min_len, max(length(str)) AS max_len FROM lc GROUP BY substring(str, 1, 1);

insert into lc values ('a'), ('bbb'), ('ab'), ('accccc'), ('baasddas'), ('bcde');
select * from lc_mv order by letter;

drop table if exists lc;
drop table if exists lc_mv;

