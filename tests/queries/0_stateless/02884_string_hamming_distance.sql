select hammingDistance('abcd', 'abcd');
drop table if exists t;
create table t
(
	s1 String,
	s2 String
) engine = MergeTree order by s1;

insert into t values ('abcdefg', 'abcdef') ('abcdefg', 'bcdefg') ('abcdefg', '');

select hammingDistance(s1, s2) from t;

select hammingDistance('abc', s2) from t;

select hammingDistance(s2, 'def') from t;

drop table t;
