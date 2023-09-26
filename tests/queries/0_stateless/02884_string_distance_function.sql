select byteHammingDistance('abcd', 'abcd');
drop table if exists t;
create table t
(
	s1 String,
	s2 String
) engine = MergeTree order by s1;

insert into t values ('abcdefg', 'abcdef') ('abcdefg', 'bcdefg') ('abcdefg', '') ('mouse', 'clickhouse');
select byteHammingDistance(s1, s2) from t;
select byteHammingDistance('abc', s2) from t;
select byteHammingDistance(s2, 'def') from t;

select mismatches(s1, s2) from t;
select mismatches('abc', s2) from t;
select mismatches(s2, 'def') from t;

select byteJaccardIndex(s1, s2) from t;
select byteEditDistance(s1, s2) from t;
select byteLevenshteinDistance(s1, s2) from t;

SELECT byteEditDistance(randomString(power(2, 17)), 'abc'); -- { serverError 131 }

drop table t;
