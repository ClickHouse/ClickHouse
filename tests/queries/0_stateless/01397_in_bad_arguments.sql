select in((1, 1, 1, 1)); -- { serverError 42 }
select in(1); -- { serverError 42 }
select in(); -- { serverError 42 }
select in(1, 2, 3); -- { serverError 42 }
