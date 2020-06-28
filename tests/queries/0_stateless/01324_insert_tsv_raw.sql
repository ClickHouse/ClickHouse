drop table if exists tsv_raw;
create table tsv_raw (a String, b Int64) engine = Memory;
insert into tsv_raw format TSVRaw "a 	1
;

select * from tsv_raw;
drop table tsv_raw;
