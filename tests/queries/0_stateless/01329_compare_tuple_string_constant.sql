SELECT tuple(1) < ''; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT tuple(1) < materialize(''); -- { serverError NO_COMMON_TYPE }
SELECT (1, 2) < '(1,3)';
SELECT (1, 2) < '(1, 1)';
