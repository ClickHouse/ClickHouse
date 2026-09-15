SELECT tokens(NULL, 1, materialize(1)) -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
