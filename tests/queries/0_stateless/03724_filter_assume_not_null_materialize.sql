SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1;
SET allow_not_comparable_types_in_comparison_functions = 0;

SELECT (assumeNotNull((NULL)), 1); -- { serverError ILLEGAL_COLUMN }

SELECT (assumeNotNull(materialize(NULL)), 1); -- { serverError ILLEGAL_COLUMN }

SELECT 1 WHERE (assumeNotNull(NULL), 1) = (1, 1); -- { serverError ILLEGAL_COLUMN }

-- Tuple comparison with Nothing-type elements returns Nullable(UInt8), yielding NULL (empty result).
SELECT 1 WHERE (assumeNotNull(materialize(NULL)), 1) = (1, 1);

SET allow_not_comparable_types_in_comparison_functions = 1;

SELECT (assumeNotNull((NULL)), 1); -- { serverError ILLEGAL_COLUMN }

SELECT (assumeNotNull(materialize(NULL)), 1); -- { serverError ILLEGAL_COLUMN }

SELECT 1 WHERE (assumeNotNull(NULL), 1) = (1, 1); -- { serverError ILLEGAL_COLUMN }

SELECT 1 WHERE (assumeNotNull(materialize(NULL)), 1) = (1, 1);
