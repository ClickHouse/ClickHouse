-- { echo }

-- Normal cases
select a, b, ntile(3) over (partition by a order by b rows between unbounded preceding and unbounded following) from(select intDiv(number,10) as a, number%10 as b from numbers(20));
select a, b, ntile(3) over (partition by a order by b) from(select intDiv(number,10) as a, number%10 as b from numbers(20));
select a, b, ntile(2) over (partition by a order by b) from(select intDiv(number,10) as a, number%10 as b from numbers(20));
select a, b, ntile(1) over (partition by a order by b) from(select intDiv(number,10) as a, number%10 as b from numbers(20));
select a, b, ntile(100) over (partition by a order by b) from(select intDiv(number,10) as a, number%10 as b from numbers(20));
select a, b, ntile(65535) over (partition by a order by b) from (select 1 as a, number as b from numbers(65535)) limit 100;



-- Bad arguments
select a, b, ntile(3.0) over (partition by a order by b) from(select intDiv(number,10) as a, number%10 as b from numbers(20)); -- { serverError BAD_ARGUMENTS }
select a, b, ntile('2') over (partition by a order by b) from(select intDiv(number,10) as a, number%10 as b from numbers(20)); -- { serverError BAD_ARGUMENTS }
select a, b, ntile(0) over (partition by a order by b) from(select intDiv(number,10) as a, number%10 as b from numbers(20)); -- { serverError BAD_ARGUMENTS }
select a, b, ntile(-2) over (partition by a order by b) from(select intDiv(number,10) as a, number%10 as b from numbers(20)); -- { serverError BAD_ARGUMENTS }
select a, b, ntile(b + 1) over (partition by a order by b) from(select intDiv(number,10) as a, number%10 as b from numbers(20)); -- { serverError BAD_ARGUMENTS }
select a, b, ntile() over (partition by a order by b) from(select intDiv(number,10) as a, number%10 as b from numbers(20)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select a, b, ntile(3, 2) over (partition by a order by b) from(select intDiv(number,10) as a, number%10 as b from numbers(20)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Bad window type
select a, b, ntile(2) over (partition by a) from(select intDiv(number,10) as a, number%10 as b from numbers(20)); -- { serverError BAD_ARGUMENTS }
select a, b, ntile(2) over (partition by a order by b rows between 4 preceding and unbounded following) from(select intDiv(number,10) as a, number%10 as b from numbers(20)); -- { serverError BAD_ARGUMENTS }
select a, b, ntile(2) over (partition by a order by b rows between unbounded preceding and 4 following) from(select intDiv(number,10) as a, number%10 as b from numbers(20)); -- { serverError BAD_ARGUMENTS }
select a, b, ntile(2) over (partition by a order by b rows between 4 preceding and 4 following) from(select intDiv(number,10) as a, number%10 as b from numbers(20));; -- { serverError BAD_ARGUMENTS }
select a, b, ntile(2) over (partition by a order by b rows between current row and 4 following) from(select intDiv(number,10) as a, number%10 as b from numbers(20));; -- { serverError BAD_ARGUMENTS }
select a, b, ntile(2) over (partition by a order by b range unbounded preceding) from(select intDiv(number,10) as a, number%10 as b from numbers(20));; -- { serverError BAD_ARGUMENTS }
