
DROP TABLE IF EXISTS bloom_filter;

CREATE TABLE bloom_filter
(
    id UInt64,
    s String,
    INDEX tok_bf (s, lower(s)) TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1
) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 8;

insert into bloom_filter select number, 'yyy,uuu' from numbers(1024);
insert into bloom_filter select number+2000, 'abc,def,zzz' from numbers(8);
insert into bloom_filter select number+3000, 'yyy,uuu' from numbers(1024);
insert into bloom_filter select number+3000, 'abcdefzzz' from numbers(1024);

set max_rows_to_read = 16;

SELECT max(id) FROM bloom_filter WHERE hasToken(s, 'abc');
SELECT max(id) FROM bloom_filter WHERE hasToken(s, 'def');
SELECT max(id) FROM bloom_filter WHERE hasToken(s, 'zzz');

-- invert result
-- this does not work as expected, reading more rows that it should
-- SELECT max(id) FROM bloom_filter WHERE NOT hasToken(s, 'yyy');

-- accessing to many rows
SELECT max(id) FROM bloom_filter WHERE hasToken(s, 'yyy'); -- { serverError 158 }

-- this syntax is not supported by tokenbf
SELECT max(id) FROM bloom_filter WHERE hasToken(s, 'zzz') == 1; -- { serverError 158 }

DROP TABLE bloom_filter;