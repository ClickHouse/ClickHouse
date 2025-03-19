-- {echoOn }

select stringCompare('abc', 'abc');
select stringCompare('abd', 'abc');
select stringCompare('abb', 'abc');
select stringCompare('abc111', 'abd');
select s, stringCompare(s, 'ab1') from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(3)));
select s, stringCompare('ab1', s) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(3)));
select s, stringCompare(s, 'ab1') from (select concat('ab', number % 6, 'cde') as s from numbers(3));
select s, stringCompare('ab1', s) from (select concat('ab', number % 6, 'cde') as s from numbers(3));
select s1, s2, stringCompare(s1, s2) from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6));
select s1, s2, stringCompare(s1, s2) from (select cast(s1 as FixedString(3)) as s1, s2 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, stringCompare(s2, s1) from (select cast(s1 as FixedString(3)) as s1, s2 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, stringCompare(s1, s2) from (select cast(s2 as FixedString(3)) as s2, cast(s1 as FixedString(3)) as s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));

select stringCompare('abc', 'abc', 0, 0, 3);
select stringCompare('abd', 'abc', 0, 0, 3);
select stringCompare('abb', 'abc', 0, 0, 3);
select stringCompare('abc', 'abd', 1, 1, 3);
select stringCompare('abc', 'abc', 4, 0, 3);
select stringCompare('abc', 'abc', 3, 4, 3);
select stringCompare('ab1', 'abc', 0, 0, 0);

select s, stringCompare(s, 'ab3', 0, 0, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));
select s, stringCompare(s, 'ab3', 6, 0, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));
select s, stringCompare(s, 'ab3', 0, 3, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));
select s, stringCompare(s, 'ab3', 6, 3, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));
select s, stringCompare('ab3', s, 0, 0, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));
select s, stringCompare('ab3', s, 3, 0, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));
select s, stringCompare('ab3', s, 0, 6, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));
select s, stringCompare('ab3', s, 3, 6, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));


select s, stringCompare(s, 'ab3', 0, 0, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));
select s, stringCompare(s, 'ab3', 6, 0, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));
select s, stringCompare(s, 'ab3', 0, 3, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));
select s, stringCompare(s, 'ab3', 6, 3, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));
select s, stringCompare('ab3', s, 0, 0, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));
select s, stringCompare('ab3', s, 3, 0, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));
select s, stringCompare('ab3', s, 0, 6, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));
select s, stringCompare('ab3', s, 3, 6, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));

select s1, s2, stringCompare(s1, s2, 0, 0, 4) from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6));
select s1, s2, stringCompare(s1, s2, 3, 0, 4) from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6));
select s1, s2, stringCompare(s1, s2, 0, 3, 4) from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6));
select s1, s2, stringCompare(s1, s2, 4, 4, 4) from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6));


select s1, s2, stringCompare(s1, s2, 0, 0, 4) from (select cast(s1 as FixedString(3)) as s1, s2 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, stringCompare(s1, s2, 3, 0, 4) from (select cast(s1 as FixedString(3)) as s1, s2 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, stringCompare(s1, s2, 0, 3, 4) from (select cast(s1 as FixedString(3)) as s1, s2 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, stringCompare(s1, s2, 4, 4, 4) from (select cast(s1 as FixedString(3)) as s1, s2 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));


select s1, s2, stringCompare(s1, s2, 0, 0, 4) from (select cast(s2 as FixedString(3)) as s2, s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, stringCompare(s1, s2, 3, 0, 4) from (select cast(s2 as FixedString(3)) as s2, s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, stringCompare(s1, s2, 0, 3, 4) from (select cast(s2 as FixedString(3)) as s2, s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, stringCompare(s1, s2, 4, 4, 4) from (select cast(s2 as FixedString(3)) as s2, s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));

select s1, s2, stringCompare(s1, s2, 0, 0, 4) from (select cast(s2 as FixedString(3)) as s2, cast(s1 as FixedString(3)) as s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, stringCompare(s1, s2, 3, 0, 4) from (select cast(s2 as FixedString(3)) as s2, cast(s1 as FixedString(3)) as s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, stringCompare(s1, s2, 0, 3, 4) from (select cast(s2 as FixedString(3)) as s2, cast(s1 as FixedString(3)) as s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, stringCompare(s1, s2, 4, 4, 4) from (select cast(s2 as FixedString(3)) as s2, cast(s1 as FixedString(3)) as s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
-- {echoOff }

select stringCompare('abc'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select stringCompare('abc', 0, 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select stringCompare('abc', 0, 1, 3); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select stringCompare('abd', 'abc', 0); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select stringCompare('abd', 'abc', 0, 0); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select stringCompare('abd', 'abc', -1);  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select stringCompare('abd', 'abc', 0, -1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select stringCompare('abd', 'abc', 0, 0, -2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select stringCompare('abd', 'abc', -1, 0, 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select stringCompare('abd', 'abc', 0, -1, 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select stringCompare('abd', 0, 'abc', 0, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }


