select in((1, 1, 1, 1)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select in(1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select in(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select in(1, 2, 3); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
