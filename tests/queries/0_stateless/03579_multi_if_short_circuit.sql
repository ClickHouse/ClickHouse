SET enable_analyzer = 1;

WITH 0 AS n SELECT multiIf(n = 0, 42, intDiv(100, n));
WITH 0 AS n SELECT multiIf(n = 1, 42, intDiv(100, n)); -- { serverError ILLEGAL_DIVISION }

WITH 0 AS n SELECT multiIf(n = 0, intDiv(100, n), 42); -- { serverError ILLEGAL_DIVISION }
WITH 0 AS n SELECT multiIf(n = 1, intDiv(100, n), 42);

WITH 0 AS n SELECT multiIf(n = 0, 42, n = 1, 24, intDiv(100, n));
WITH 0 AS n SELECT multiIf(n = 1, 42, n = 0, 24, intDiv(100, n));
WITH 0 AS n SELECT multiIf(n = 0, 42, n = 1, intDiv(100, n), 24);
WITH 0 AS n SELECT multiIf(n = 1, 42, n = 0, intDiv(100, n), 24); -- { serverError ILLEGAL_DIVISION }

WITH 0 AS n SELECT multiIf(n = 0, intDiv(100, n), n = 1, 42, intDiv(100, n)); -- { serverError ILLEGAL_DIVISION }
WITH 0 AS n SELECT multiIf(n = 1, intDiv(100, n), n = 0, 42, intDiv(100, n));
WITH 0 AS n SELECT multiIf(n = 0, intDiv(100, n), n = 1, intDiv(100, n), 42); -- { serverError ILLEGAL_DIVISION }
WITH 0 AS n SELECT multiIf(n = 1, intDiv(100, n), n = 0, intDiv(100, n), 42); -- { serverError ILLEGAL_DIVISION }

SELECT if(1, 1, (SELECT number FROM numbers(2)));
SELECT if(0, 1, (SELECT number FROM numbers(2))); -- { serverError INCORRECT_RESULT_OF_SCALAR_SUBQUERY }
SELECT if(1, 1, (SELECT blahblah FROM numbers(1))); -- { serverError UNKNOWN_IDENTIFIER }

SELECT if(1, 1, COLUMNS('num')) from numbers(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 0 ? not_existing_column : 1 FROM numbers(1);
SELECT 1 ? not_existing_column : 1 FROM numbers(1); -- { serverError UNKNOWN_IDENTIFIER }

SELECT multiIf(0, not_existing_column, 1, 1, 1) FROM numbers(1);
SELECT multiIf(1, not_existing_column, 1, 1, 1) FROM numbers(1); -- { serverError UNKNOWN_IDENTIFIER }

SELECT if(1, 'a', cast(1, assumeNotNull((SELECT 'String'))));
