select 1 as c1, 2 as c2, 3 as c3 union all (select 1 as c1, 2 as c2, 3 as c3 union all select 1 as c1, 2 as c2) -- { serverError UNION_ALL_RESULT_STRUCTURES_MISMATCH }
