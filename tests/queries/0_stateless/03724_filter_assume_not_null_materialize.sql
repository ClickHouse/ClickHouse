SET allow_not_comparable_types_in_comparison_functions = 0;

SELECT (assumeNotNull((NULL)), 1); -- { serverError ILLEGAL_COLUMN }

SELECT (assumeNotNull(materialize(NULL)), 1); -- { serverError ILLEGAL_COLUMN }

SELECT 1 WHERE (assumeNotNull(NULL), 1) = (1, 1); -- { serverError ILLEGAL_COLUMN }

-- With the new analyzer, tuple comparison with Nothing-type elements returns Nullable(UInt8), yielding NULL (empty result).
-- With the old analyzer, the filter type is resolved as Nothing, causing ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER.
SET enable_analyzer = 1;
SELECT 1 WHERE (assumeNotNull(materialize(NULL)), 1) = (1, 1);
SET enable_analyzer = 0;
SELECT 1 WHERE (assumeNotNull(materialize(NULL)), 1) = (1, 1); -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }

SET allow_not_comparable_types_in_comparison_functions = 1;

SELECT (assumeNotNull((NULL)), 1); -- { serverError ILLEGAL_COLUMN }

SELECT (assumeNotNull(materialize(NULL)), 1); -- { serverError ILLEGAL_COLUMN }

SELECT 1 WHERE (assumeNotNull(NULL), 1) = (1, 1); -- { serverError ILLEGAL_COLUMN }

SET enable_analyzer = 1;
SELECT 1 WHERE (assumeNotNull(materialize(NULL)), 1) = (1, 1);
SET enable_analyzer = 0;
SELECT 1 WHERE (assumeNotNull(materialize(NULL)), 1) = (1, 1); -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }
