drop table if exists tab;

create table tab (id UInt32, haystack String, pattern String) engine = MergeTree() order by id;
insert into tab values (1, 'aaaxxxaa\0xxx', 'x');

select countSubstrings('aaaxxxaa\0xxx', pattern) from tab where id = 1;
select countSubstringsCaseInsensitive('aaaxxxaa\0xxx', pattern) from tab where id = 1;
select countSubstringsCaseInsensitiveUTF8('aaaxxxaa\0xxx', pattern) from tab where id = 1;

select countSubstrings(haystack, pattern) from tab where id = 1;
select countSubstringsCaseInsensitive(haystack, pattern) from tab where id = 1;
select countSubstringsCaseInsensitiveUTF8(haystack, pattern) from tab where id = 1;

insert into tab values (2, 'aaaaa\0x', 'x');

select position('aaaaa\0x', pattern) from tab where id = 2;
select positionCaseInsensitive('aaaaa\0x', pattern) from tab where id = 2;
select positionCaseInsensitiveUTF8('aaaaa\0x', pattern) from tab where id = 2;

select position(haystack, pattern) from tab where id = 2;
select positionCaseInsensitive(haystack, pattern) from tab where id = 2;
select positionCaseInsensitiveUTF8(haystack, pattern) from tab where id = 2;

drop table if exists tab;
