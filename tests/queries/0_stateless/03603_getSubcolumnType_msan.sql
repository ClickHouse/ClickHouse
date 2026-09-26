-- https://github.com/ClickHouse/ClickHouse/issues/86279
SELECT '{}'::JSON x QUALIFY x.^c0 = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT, UNKNOWN_IDENTIFIER }
