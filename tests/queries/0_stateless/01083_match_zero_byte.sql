-- Tags: no-fasttest
-- no-fasttest: Requires vectorscan
select match('a key="v" ', 'key="(.*?)"');
select match(materialize('a key="v" '), 'key="(.*?)"');

select match('\0 key="v" ', 'key="(.*?)"');
select match(materialize('\0 key="v" '), 'key="(.*?)"');

select multiMatchAny('\0 key="v" ', ['key="(.*?)"']);
select multiMatchAny(materialize('\0 key="v" '), ['key="(.*?)"']);

select unhex('34') || ' key="v" ' as haystack, length(haystack), extract( haystack, 'key="(.*?)"') as needle;
-- works, result = v

select unhex('00') || ' key="v" ' as haystack, length(haystack), extract( haystack, 'key="(.*?)"') as needle;
-- before fix: returns nothing (zero-byte in the begining of haystack)

select number as char_code,  extract( char(char_code) || ' key="v" ' as haystack, 'key="(.*?)"') as needle from numbers(256);
-- every other chars codes (except of zero byte) works ok
