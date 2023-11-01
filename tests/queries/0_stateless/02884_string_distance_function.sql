select 'const arguments byteHammingDistance';
select byteHammingDistance('abcd', 'abcd');
select 'const arguments editDistance';
select editDistance('clickhouse', 'mouse');
/*select 'const arguments jaccardIndex';
select jaccardIndex('clickhouse', 'mouse');*/

drop table if exists t;
create table t
(
	s1 String,
	s2 String
) engine = MergeTree order by s1;

insert into t values ('abcdefg', 'abcdef') ('abcdefg', 'bcdefg') ('abcdefg', '') ('mouse', 'clickhouse');
select 'byteHammingDistance';
select byteHammingDistance(s1, s2) from t;
select 'byteHammingDistance(const, non const)';
select byteHammingDistance('abc', s2) from t;
select 'byteHammingDistance(non const, const)';
select byteHammingDistance(s2, 'def') from t;

select 'mismatches(alias)';
select mismatches(s1, s2) from t;
select mismatches('abc', s2) from t;
select mismatches(s2, 'def') from t;

/*select 'byteJaccardIndex';
select byteJaccardIndex(s1, s2) from t;*/
select 'editDistance';
select editDistance(s1, s2) from t;
select 'levenshteinDistance';
select levenshteinDistance(s1, s2) from t;

SELECT editDistance(randomString(power(2, 17)), 'abc'); -- { serverError TOO_LARGE_STRING_SIZE}

drop table t;
