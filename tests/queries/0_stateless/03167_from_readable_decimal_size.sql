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
-- Invalid input - Binary size unit is not accepted
SELECT fromReadableDecimalSize('1 KiB'); -- { serverError CANNOT_PARSE_TEXT }
-- Invalid input - Negative sizes are not allowed
SELECT fromReadableDecimalSize('-1 KB'); -- { serverError BAD_ARGUMENTS }
-- Invalid input - Input too large to fit in UInt64
SELECT fromReadableDecimalSize('1000 EB'); -- { serverError BAD_ARGUMENTS }


-- OR NULL
-- Works as the regular version when inputs are correct
SELECT
    arrayJoin(['1 B', '1 KB', '1 MB', '1 GB', '1 TB', '1 PB', '1 EB']) AS readable_sizes,
    fromReadableDecimalSizeOrNull(readable_sizes) AS filesize;

-- Returns NULL on invalid values
SELECT
    arrayJoin(['invalid', '1 Joe', '1 KiB', ' 1 GB', '1 TB with fries']) AS readable_sizes,
    fromReadableDecimalSizeOrNull(readable_sizes) AS filesize;


-- OR ZERO
-- Works as the regular version when inputs are correct
SELECT
    arrayJoin(['1 B', '1 KB', '1 MB', '1 GB', '1 TB', '1 PB', '1 EB']) AS readable_sizes,
    fromReadableDecimalSizeOrZero(readable_sizes) AS filesize;

-- Returns NULL on invalid values
SELECT
    arrayJoin(['invalid', '1 Joe', '1 KiB', ' 1 GiB', '1 TiB with fries']) AS readable_sizes,
    fromReadableDecimalSizeOrZero(readable_sizes) AS filesize;

