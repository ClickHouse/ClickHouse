SELECT * FROM globalIn('a', 1); -- { serverError UNKNOWN_FUNCTION }
SELECT * FROM plus(1, 2); -- { serverError UNKNOWN_FUNCTION }
SELECT * FROM negate(x); -- { serverError UNKNOWN_FUNCTION }

SELECT not((SELECT * AND(16)) AND 1);
