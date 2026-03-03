SET allow_not_comparable_types_in_comparison_functions = 0;

SELECT (assumeNotNull((NULL)), 1); -- { serverError ILLEGAL_COLUMN }

SELECT (assumeNotNull(materialize(NULL)), 1); -- { serverError ILLEGAL_COLUMN }

SELECT 1 WHERE (assumeNotNull(NULL), 1) = (1, 1); -- { serverError ILLEGAL_COLUMN }

SELECT 1 WHERE (assumeNotNull(materialize(NULL)), 1) = (1, 1); -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }

SET allow_not_comparable_types_in_comparison_functions = 1;

SELECT (assumeNotNull((NULL)), 1); -- { serverError ILLEGAL_COLUMN }

SELECT (assumeNotNull(materialize(NULL)), 1); -- { serverError ILLEGAL_COLUMN }

SELECT 1 WHERE (assumeNotNull(NULL), 1) = (1, 1); -- { serverError ILLEGAL_COLUMN }

SELECT 1 WHERE (assumeNotNull(materialize(NULL)), 1) = (1, 1); -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }
