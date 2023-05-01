set allow_suspicious_low_cardinality_types = 1;
drop table if exists lc_00688;
create table lc_00688 (str StringWithDictionary, val UInt8WithDictionary) engine = MergeTree order by tuple();
insert into lc_00688 values ('a', 1), ('b', 2);
select str, str in ('a', 'd') from lc_00688;
select val, val in (1, 3) from lc_00688;
select str, str in (select arrayJoin(['a', 'd'])) from lc_00688;
select val, val in (select arrayJoin([1, 3])) from lc_00688;
select str, str in (select str from lc_00688) from lc_00688;
select val, val in (select val from lc_00688) from lc_00688;
drop table if exists lc_00688;

drop table if exists ary_lc_null;
CREATE TABLE ary_lc_null (i int, v Array(LowCardinality(Nullable(String)))) ENGINE = MergeTree() ORDER BY i ;
INSERT INTO ary_lc_null VALUES (1, ['1']);
SELECT v FROM ary_lc_null WHERE v IN (SELECT v FROM ary_lc_null);
drop table if exists ary_lc_null;
