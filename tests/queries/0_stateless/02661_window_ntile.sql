-- { echo }

-- Normal cases
select a, b, ntile(3) over (partition by a order by b rows between unbounded preceding and current row) from(select intDiv(number,10) as a, number%10 as b from numbers(20));
select a, b, ntile(2) over (partition by a order by b rows between unbounded preceding and current row) from(select intDiv(number,10) as a, number%10 as b from numbers(20));
select a, b, ntile(1) over (partition by a order by b rows between unbounded preceding and current row) from(select intDiv(number,10) as a, number%10 as b from numbers(20));
select a, b, ntile(100) over (partition by a order by b rows between unbounded preceding and current row) from(select intDiv(number,10) as a, number%10 as b from numbers(20));

-- Bad arguments
select a, b, ntile(3.0) over (partition by a order by b rows between unbounded preceding and current row) from(select intDiv(number,10) as a, number%10 as b from numbers(20)); -- { serverError 36 }
select a, b, ntile('2') over (partition by a order by b rows between unbounded preceding and current row) from(select intDiv(number,10) as a, number%10 as b from numbers(20)); -- { serverError 36 }
select a, b, ntile(0) over (partition by a order by b rows between unbounded preceding and current row) from(select intDiv(number,10) as a, number%10 as b from numbers(20)); -- { serverError 36 }
select a, b, ntile(-2) over (partition by a order by b rows between unbounded preceding and current row) from(select intDiv(number,10) as a, number%10 as b from numbers(20)); -- { serverError 36 }
select a, b, ntile(b + 1) over (partition by a order by b rows between unbounded preceding and current row) from(select intDiv(number,10) as a, number%10 as b from numbers(20)); -- { serverError 36 }

-- Bad window type
select a, b, ntile(2) over (partition by a) from(select intDiv(number,10) as a, number%10 as b from numbers(20)); -- { serverError 36 }
select a, b, ntile(2) over (partition by a order by b rows between 4 preceding and current row) from(select intDiv(number,10) as a, number%10 as b from numbers(20)); -- { serverError 36 }
select a, b, ntile(2) over (partition by a order by b rows between unbounded preceding and 4 following) from(select intDiv(number,10) as a, number%10 as b from numbers(20)); -- { serverError 36 }
select a, b, ntile(2) over (partition by a order by b rows between 4 preceding and 4 following) from(select intDiv(number,10) as a, number%10 as b from numbers(20));; -- { serverError 36 }
select a, b, ntile(2) over (partition by a order by b rows between current row  and 4 following) from(select intDiv(number,10) as a, number%10 as b from numbers(20));; -- { serverError 36 }
select a, b, ntile(2) over (partition by a order by b range unbounded preceding) from(select intDiv(number,10) as a, number%10 as b from numbers(20));; -- { serverError 36 }
