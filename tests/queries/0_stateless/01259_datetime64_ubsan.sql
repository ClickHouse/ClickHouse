select now64(10); -- { serverError ARGUMENT_OUT_OF_BOUND }
select length(toString(now64(9)));
