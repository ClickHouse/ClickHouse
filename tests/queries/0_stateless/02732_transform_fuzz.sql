SELECT caseWithExpr(arrayReduce(NULL, []), []); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
