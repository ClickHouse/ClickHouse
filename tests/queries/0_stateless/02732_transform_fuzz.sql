SELECT caseWithExpr(arrayReduce(NULL, []), []); -- { serverError BAD_ARGUMENTS }
