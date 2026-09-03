select m[0], materialize(map('key', 42)) as m; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
