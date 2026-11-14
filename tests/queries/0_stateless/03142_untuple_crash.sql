SET enable_analyzer=1;
SELECT untuple(x -> 0) -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
