select a, max(b) max_b, min(b) min_b from
(
	select 1 as a, 10 as b
	union all
	select 1 as a, 20 as b
	union all
	select 1 as a, 30 as b
)
group by a
having b = 10 or max(b) = 30; -- { serverError 215 }

select a, max(b) max_b, min(b) min_b from
(
	select 1 as a, 10 as b
	union all
	select 1 as a, 20 as b
	union all
	select 1 as a, 30 as b
)
group by a
having b = 10; -- { serverError 215 }
