select now64(10); -- { serverError 69 }
select length(toString(now64(9)));
