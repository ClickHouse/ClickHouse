select 'const arguments byteHammingDistance';
select byteHammingDistance('abcd', 'abcd');
select 'const arguments editDistance';
select editDistance('clickhouse', 'mouse');

select 'const arguments stringJaccardIndex';
select stringJaccardIndex('clickhouse', 'mouse');

drop table if exists t;
create table t
(
	s1 String,
	s2 String
) engine = MergeTree order by s1;

insert into t values ('abcdefg', 'abcdef') ('abcdefg', 'bcdefg') ('abcdefg', '') ('mouse', 'clickhouse');
select 'byteHammingDistance';
select byteHammingDistance(s1, s2) FROM t ORDER BY s1, s2;
select 'byteHammingDistance(const, non const)';
select byteHammingDistance('abc', s2) FROM t ORDER BY s1, s2;
select 'byteHammingDistance(non const, const)';
select byteHammingDistance(s2, 'def') FROM t ORDER BY s1, s2;

select 'mismatches(alias)';
select mismatches(s1, s2) FROM t ORDER BY s1, s2;
select mismatches('abc', s2) FROM t ORDER BY s1, s2;
select mismatches(s2, 'def') FROM t ORDER BY s1, s2;

select 'stringJaccardIndex';
select stringJaccardIndex(s1, s2) FROM t ORDER BY s1, s2;
select stringJaccardIndexUTF8(s1, s2) FROM t ORDER BY s1, s2;

-- we do not perform full UTF8 validation, so sometimes it just returns some result
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\x48\x65\x6C'));
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xFF\xFF\xFF\xFF'));
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\x41\xE2\x82\xAC'));
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xF0\x9F\x99\x82'));
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xFF'));
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xC2\x01')); -- { serverError BAD_ARGUMENTS }
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xC1\x81')); -- { serverError BAD_ARGUMENTS }
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xF0\x80\x80\x41')); -- { serverError BAD_ARGUMENTS }
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xC0\x80')); -- { serverError BAD_ARGUMENTS }
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xD8\x00 ')); -- { serverError BAD_ARGUMENTS }
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xDC\x00')); -- { serverError BAD_ARGUMENTS }

SELECT stringJaccardIndexUTF8('😃🌍', '🙃😃🌑'), stringJaccardIndex('😃🌍', '🙃😃🌑');

select 'editDistance';
select editDistance(s1, s2) FROM t ORDER BY s1, s2;
select 'levenshteinDistance';
select levenshteinDistance(s1, s2) FROM t ORDER BY s1, s2;

SELECT editDistance(randomString(power(2, 17)), 'abc'); -- { serverError TOO_LARGE_STRING_SIZE}

drop table t;
