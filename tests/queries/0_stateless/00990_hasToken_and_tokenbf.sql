DROP TABLE IF EXISTS bloom_filter;

CREATE TABLE bloom_filter
(
    id UInt64,
    s String,
    INDEX tok_bf (s, lower(s)) TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1
) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';

insert into bloom_filter select number, 'yyy,uuu' from numbers(1024);
insert into bloom_filter select number+2000, 'abc,def,zzz' from numbers(8);
insert into bloom_filter select number+3000, 'yyy,uuu' from numbers(1024);
insert into bloom_filter select number+3000, 'abcdefzzz' from numbers(1024);

SELECT max(id) FROM bloom_filter WHERE hasToken(s, 'abc,def,zzz'); -- { serverError BAD_ARGUMENTS }
SELECT max(id) FROM bloom_filter WHERE hasTokenCaseInsensitive(s, 'abc,def,zzz'); -- { serverError BAD_ARGUMENTS }

SELECT max(id) FROM bloom_filter WHERE hasTokenOrNull(s, 'abc,def,zzz');
SELECT max(id) FROM bloom_filter WHERE hasTokenCaseInsensitiveOrNull(s, 'abc,def,zzz');

-- as table "bloom_filter" but w/o index_granularity_bytes
drop table if exists bloom_filter2;
create table bloom_filter2
(
    id UInt64,
    s String,
    index tok_bf3 (s, lower(s)) type tokenbf_v1(512, 3, 0) GRANULARITY 1
) engine = MergeTree() order by id settings index_granularity = 8;

insert into bloom_filter2 select number, 'yyy,uuu' from numbers(1024);
insert into bloom_filter2 select number+2000, 'ABC,def,zzz' from numbers(8);
insert into bloom_filter2 select number+3000, 'yyy,uuu' from numbers(1024);
insert into bloom_filter2 select number+3000, 'abcdefzzz' from numbers(1024);

set max_rows_to_read = 16;

SELECT max(id) FROM bloom_filter WHERE hasToken(s, 'abc');
SELECT max(id) FROM bloom_filter WHERE hasTokenOrNull(s, 'abc');
SELECT max(id) FROM bloom_filter WHERE hasToken(s, 'ABC');
select max(id) from bloom_filter where hasTokenCaseInsensitive(s, 'ABC');
select max(id) from bloom_filter where hasTokenCaseInsensitiveOrNull(s, 'ABC');
SELECT max(id) FROM bloom_filter WHERE hasToken(s, 'def');
SELECT max(id) FROM bloom_filter WHERE hasToken(s, 'zzz');
select max(id) from bloom_filter where hasTokenCaseInsensitive(s, 'zZz');

select max(id) from bloom_filter2 where hasToken(s, 'ABC');
select max(id) from bloom_filter2 where hasToken(s, 'abc');
select max(id) from bloom_filter2 where hasTokenCaseInsensitive(s, 'abc');
select max(id) from bloom_filter2 where hasTokenCaseInsensitive(s, 'ABC');

-- invert result
-- this does not work as expected, reading more rows that it should
-- SELECT max(id) FROM bloom_filter WHERE NOT hasToken(s, 'yyy');

-- accessing to many rows
SELECT max(id) FROM bloom_filter WHERE hasToken(s, 'yyy'); -- { serverError TOO_MANY_ROWS }

-- this syntax is not supported by tokenbf
SELECT max(id) FROM bloom_filter WHERE hasToken(s, 'zzz') == 1; -- { serverError TOO_MANY_ROWS }

DROP TABLE bloom_filter;

-- AST fuzzer crash, issue #54541
CREATE TABLE tab (row_id UInt32, str String, INDEX idx str TYPE tokenbf_v1(256, 2, 0)) ENGINE = MergeTree ORDER BY row_id;
INSERT INTO tab VALUES (0, 'a');
SELECT * FROM tab WHERE str == 'else' AND 1.0;
DROP TABLE tab;
