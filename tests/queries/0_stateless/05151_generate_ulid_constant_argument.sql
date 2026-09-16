-- Tags: no-fasttest
-- no-fasttest: `generateULID` needs the ulid library, which the fast test build does not include.

-- `generateULID` must produce an independent identifier per row: the argument exists to get one per
-- call, so a constant argument must not collapse the whole query into a single draw.

SELECT uniqExact(generateULID('x')) FROM numbers(100);
SELECT uniqExact(generateULID()) FROM numbers(100);
SELECT generateULID('x') != generateULID('y');
SELECT length(generateULID('x')) FROM numbers(2);

-- The argument is ignored, so an ignored `NULL` still produces an identifier.
SELECT length(generateULID(NULL));
SELECT uniqExact(generateULID(NULL)) FROM numbers(100);
