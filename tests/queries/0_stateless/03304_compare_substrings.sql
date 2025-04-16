-- Negative tests

-- Five arguments are expected
select compareSubstrings(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select compareSubstrings('abc'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select compareSubstrings('abc', 'abc'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select compareSubstrings('abc', 'abc', 0); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select compareSubstrings('abc', 'abc', 0, 0); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select compareSubstrings('abc', 'abc', 0, 0, 0, 0); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- 1st/2nd argument must be string, 3rd/4th/5th argument must be integer
select compareSubstrings(0, 'abc', 0, 0, 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select compareSubstrings('abc', 0, 0, 0, 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select compareSubstrings('abc', 'abc', 'abc', 0, 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select compareSubstrings('abc', 'abc', 0, 'abc', 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select compareSubstrings('abc', 'abc', 0, 0, 'abc'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- 3rd, 4th, 5th argument must be non-negative
select compareSubstrings('abc', 'abc', -1, 0, 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select compareSubstrings('abc', 'abc', 0, -1, 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select compareSubstrings('abc', 'abc', 0, 0, -1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- {echoOn }

select compareSubstrings('abc', 'abc', 0, 0, 3);
select compareSubstrings('abd', 'abc', 0, 0, 3);
select compareSubstrings('abb', 'abc', 0, 0, 3);
select compareSubstrings('abc', 'abd', 1, 1, 3);
select compareSubstrings('abc', 'abc', 4, 0, 3);
select compareSubstrings('abc', 'abc', 3, 4, 3);
select compareSubstrings('ab1', 'abc', 0, 0, 0);

select s, compareSubstrings(s, 'ab3', 0, 0, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));
select s, compareSubstrings(s, 'ab3', 6, 0, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));
select s, compareSubstrings(s, 'ab3', 0, 3, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));
select s, compareSubstrings(s, 'ab3', 6, 3, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));
select s, compareSubstrings('ab3', s, 0, 0, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));
select s, compareSubstrings('ab3', s, 3, 0, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));
select s, compareSubstrings('ab3', s, 0, 6, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));
select s, compareSubstrings('ab3', s, 3, 6, 3) from (select cast(s as FixedString(6)) as s from (select concat('ab', number % 6, 'cde') as s from numbers(8)));


select s, compareSubstrings(s, 'ab3', 0, 0, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));
select s, compareSubstrings(s, 'ab3', 6, 0, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));
select s, compareSubstrings(s, 'ab3', 0, 3, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));
select s, compareSubstrings(s, 'ab3', 6, 3, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));
select s, compareSubstrings('ab3', s, 0, 0, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));
select s, compareSubstrings('ab3', s, 3, 0, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));
select s, compareSubstrings('ab3', s, 0, 6, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));
select s, compareSubstrings('ab3', s, 3, 6, 3) from (select concat('ab', number % 6, 'cde') as s from numbers(8));

select s1, s2, compareSubstrings(s1, s2, 0, 0, 4) from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6));
select s1, s2, compareSubstrings(s1, s2, 3, 0, 4) from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6));
select s1, s2, compareSubstrings(s1, s2, 0, 3, 4) from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6));
select s1, s2, compareSubstrings(s1, s2, 4, 4, 4) from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6));


select s1, s2, compareSubstrings(s1, s2, 0, 0, 4) from (select cast(s1 as FixedString(3)) as s1, s2 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, compareSubstrings(s1, s2, 3, 0, 4) from (select cast(s1 as FixedString(3)) as s1, s2 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, compareSubstrings(s1, s2, 0, 3, 4) from (select cast(s1 as FixedString(3)) as s1, s2 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, compareSubstrings(s1, s2, 4, 4, 4) from (select cast(s1 as FixedString(3)) as s1, s2 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));


select s1, s2, compareSubstrings(s1, s2, 0, 0, 4) from (select cast(s2 as FixedString(3)) as s2, s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, compareSubstrings(s1, s2, 3, 0, 4) from (select cast(s2 as FixedString(3)) as s2, s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, compareSubstrings(s1, s2, 0, 3, 4) from (select cast(s2 as FixedString(3)) as s2, s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, compareSubstrings(s1, s2, 4, 4, 4) from (select cast(s2 as FixedString(3)) as s2, s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));

select s1, s2, compareSubstrings(s1, s2, 0, 0, 4) from (select cast(s2 as FixedString(3)) as s2, cast(s1 as FixedString(3)) as s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, compareSubstrings(s1, s2, 3, 0, 4) from (select cast(s2 as FixedString(3)) as s2, cast(s1 as FixedString(3)) as s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, compareSubstrings(s1, s2, 0, 3, 4) from (select cast(s2 as FixedString(3)) as s2, cast(s1 as FixedString(3)) as s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));
select s1, s2, compareSubstrings(s1, s2, 4, 4, 4) from (select cast(s2 as FixedString(3)) as s2, cast(s1 as FixedString(3)) as s1 from (select concat('ab', number % 3) as s1, concat('ab', number % 4) as s2 from numbers(6)));

-- {echoOff }
