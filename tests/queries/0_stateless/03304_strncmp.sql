-- {echoOn }
select strncmp('abc', 0, 'abc', 0, 3);
select strncmp('abd', 0, 'abc', 0, 3);
select strncmp('abb', 0, 'abc', 0, 3);
select strncmp('abc', 1, 'abd', 1, 3);
select strncmp('abc', 4, 'abc', 0, 3);
select strncmp('abc', 3, 'abc', 4, 3);

select s, strncmp(s, 0, 'ab3', 0, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));
select s, strncmp(s, 6, 'ab3', 0, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));
select s, strncmp(s, 0, 'ab3', 3, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));
select s, strncmp(s, 6, 'ab3', 3, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));
select s, strncmp('ab3', 0, s, 0, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));
select s, strncmp('ab3', 3, s, 0, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));
select s, strncmp('ab3', 0, s, 6, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));
select s, strncmp('ab3', 3, s, 6, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));


select s, strncmp(s, 0, 'ab3', 0, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));
select s, strncmp(s, 6, 'ab3', 0, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));
select s, strncmp(s, 0, 'ab3', 3, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));
select s, strncmp(s, 6, 'ab3', 3, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));
select s, strncmp('ab3', 0, s, 0, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));
select s, strncmp('ab3', 3, s, 0, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));
select s, strncmp('ab3', 0, s, 6, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));
select s, strncmp('ab3', 3, s, 6, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));

select s1, s2, strncmp(s1, 0, s2, 0, 4) from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6));
select s1, s2, strncmp(s1, 3, s2, 0, 4) from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6));
select s1, s2, strncmp(s1, 0, s2, 3, 4) from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6));
select s1, s2, strncmp(s1, 4, s2, 4, 4) from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6));


select s1, s2, strncmp(s1, 0, s2, 0, 4) from (select cast(s1 as FixedString(3)) as s1, s2 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, strncmp(s1, 3, s2, 0, 4) from (select cast(s1 as FixedString(3)) as s1, s2 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, strncmp(s1, 0, s2, 3, 4) from (select cast(s1 as FixedString(3)) as s1, s2 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, strncmp(s1, 4, s2, 4, 4) from (select cast(s1 as FixedString(3)) as s1, s2 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));


select s1, s2, strncmp(s1, 0, s2, 0, 4) from (select cast(s2 as FixedString(3)) as s2, s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, strncmp(s1, 3, s2, 0, 4) from (select cast(s2 as FixedString(3)) as s2, s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, strncmp(s1, 0, s2, 3, 4) from (select cast(s2 as FixedString(3)) as s2, s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, strncmp(s1, 4, s2, 4, 4) from (select cast(s2 as FixedString(3)) as s2, s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));

select s1, s2, strncmp(s1, 0, s2, 0, 4) from (select cast(s2 as FixedString(3)) as s2, cast(s1 as FixedString(3)) as s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, strncmp(s1, 3, s2, 0, 4) from (select cast(s2 as FixedString(3)) as s2, cast(s1 as FixedString(3)) as s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, strncmp(s1, 0, s2, 3, 4) from (select cast(s2 as FixedString(3)) as s2, cast(s1 as FixedString(3)) as s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, strncmp(s1, 4, s2, 4, 4) from (select cast(s2 as FixedString(3)) as s2, cast(s1 as FixedString(3)) as s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
-- {echoOff }

