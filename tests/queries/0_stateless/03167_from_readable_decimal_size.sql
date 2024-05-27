-- Should be the inverse of formatReadableDecimalSize
SELECT formatReadableDecimalSize(fromReadableDecimalSize('1 B'));
SELECT formatReadableDecimalSize(fromReadableDecimalSize('1 KB'));
SELECT formatReadableDecimalSize(fromReadableDecimalSize('1 MB'));
SELECT formatReadableDecimalSize(fromReadableDecimalSize('1 GB'));
SELECT formatReadableDecimalSize(fromReadableDecimalSize('1 TB'));
SELECT formatReadableDecimalSize(fromReadableDecimalSize('1 PB'));
SELECT formatReadableDecimalSize(fromReadableDecimalSize('1 EB'));

-- Is case-insensitive
SELECT formatReadableDecimalSize(fromReadableDecimalSize('1 mb'));

-- Should be able to parse decimals
SELECT fromReadableDecimalSize('1.00 KB');    -- 1024
SELECT fromReadableDecimalSize('3.00 KB');    -- 3072

-- Should be able to parse negative numbers
SELECT fromReadableDecimalSize('-1.00 KB');    -- 1024

-- Infix whitespace is ignored
SELECT fromReadableDecimalSize('1    KB');
SELECT fromReadableDecimalSize('1KB');

-- Can parse LowCardinality
SELECT fromReadableDecimalSize(toLowCardinality('1 KB'));

-- Can parse nullable fields
SELECT fromReadableDecimalSize(toNullable('1 KB'));

-- Can parse non-const columns fields
SELECT fromReadableDecimalSize(materialize('1 KB'));

-- Output is NULL if NULL arg is passed
SELECT fromReadableDecimalSize(NULL);

-- ERRORS
-- No arguments
SELECT fromReadableDecimalSize(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- Too many arguments
SELECT fromReadableDecimalSize('1 B', '2 B'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- Wrong Type
SELECT fromReadableDecimalSize(12); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- Invalid input - overall garbage
SELECT fromReadableDecimalSize('oh no'); -- { serverError CANNOT_PARSE_NUMBER }
-- Invalid input - unknown unit
SELECT fromReadableDecimalSize('12.3 rb'); -- { serverError CANNOT_PARSE_TEXT }
-- Invalid input - Leading whitespace
SELECT fromReadableDecimalSize(' 1 B'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
-- Invalid input - Trailing characters
SELECT fromReadableDecimalSize('1 B leftovers'); -- { serverError UNEXPECTED_DATA_AFTER_PARSED_VALUE }
