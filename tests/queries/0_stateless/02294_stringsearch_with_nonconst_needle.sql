drop table if exists non_const_needle;

create table non_const_needle
  (id UInt32, haystack String, needle String)
  engine = MergeTree()
  order by id;

-- 1 - 33: LIKE-syntax, 34-37: re2-syntax
insert into non_const_needle values (1, 'Hello', '') (2, 'Hello', '%') (3, 'Hello', '%%') (4, 'Hello', '%%%') (5, 'Hello', '%_%') (6, 'Hello', '_') (7, 'Hello', '_%') (8, 'Hello', '%_') (9, 'Hello', 'H%o') (10, 'hello', 'H%0') (11, 'hello', 'h%o') (12, 'Hello', 'h%o') (13, 'OHello', '%lhell%') (14, 'OHello', '%hell%') (15, 'hEllo', '%HEL%') (16, 'abcdef', '%aBc%def%') (17, 'ABCDDEF', '%abc%def%') (18, 'Abc\nDef', '%abc%def%') (19, 'abc\ntdef', '%abc%def%') (20, 'abct\ndef', '%abc%dEf%') (21, 'abc\n\ndeF', '%abc%def%') (22, 'abc\n\ntdef', '%abc%deF%') (23, 'Abc\nt\ndef', '%abc%def%') (24, 'abct\n\ndef', '%abc%def%') (25, 'ab\ndef', '%Abc%def%') (26, 'aBc\nef', '%ABC%DEF%') (27, 'ёЁё', 'Ё%Ё') (28, 'ощщЁё', 'Щ%Ё') (29, 'ощЩЁё', '%Щ%Ё') (30, 'Щущпандер', '%щп%е%') (31, 'Щущпандер', '%щП%е%') (32, 'ощщЁё', '%щ%') (33, 'ощЩЁё', '%ё%') (34, 'Hello', '.*') (35, 'Hello', '.*ell.*') (36, 'Hello', 'o$') (37, 'Hello', 'hE.*lO');

select 'LIKE';
select id, haystack, needle, like(haystack, needle)
  from non_const_needle
  order by id;

select 'NOT LIKE';
select id, haystack, needle, not like(haystack, needle)
  from non_const_needle
  order by id;

select 'ILIKE';
select id, haystack, needle, ilike(haystack, needle)
  from non_const_needle
  order by id;

select 'NOT ILIKE';
select id, haystack, needle, not ilike(haystack, needle)
  from non_const_needle
  order by id;

select 'MATCH';
select id, haystack, needle, match(haystack, needle)
  from non_const_needle
  order by id;

drop table if exists non_const_needle;
