SELECT tokens(NULL, 1, materialize(1)) -- { serverError ILLEGAL_COLUMN }
