SET enable_analyzer = 1;
SELECT [1, (x -> 1)]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT (1, (x -> 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT map(1, (x -> 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT [1, lambda(x, 1)]; -- { serverError UNKNOWN_IDENTIFIER }
SELECT (1, lambda(x, 1)); -- { serverError UNKNOWN_IDENTIFIER }
SELECT map(1, lambda(x, 1)); -- { serverError UNKNOWN_IDENTIFIER }
