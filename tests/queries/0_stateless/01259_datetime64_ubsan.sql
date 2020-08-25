select now64(10); -- { serverError 407 }
select length(toString(now64(9)));
