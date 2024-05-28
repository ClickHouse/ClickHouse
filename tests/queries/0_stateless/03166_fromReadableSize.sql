-- Should be kept in sync with 03167_fromReadableDecimalSize.sql

-- Should be the inverse of formatReadableSize
SELECT formatReadableSize(fromReadableSize('1 B'));
SELECT formatReadableSize(fromReadableSize('1 KiB'));
SELECT formatReadableSize(fromReadableSize('1 MiB'));
SELECT formatReadableSize(fromReadableSize('1 GiB'));
SELECT formatReadableSize(fromReadableSize('1 TiB'));
SELECT formatReadableSize(fromReadableSize('1 PiB'));
SELECT formatReadableSize(fromReadableSize('1 EiB'));

-- Is case-insensitive
SELECT formatReadableSize(fromReadableSize('1 mIb'));

-- Should be able to parse decimals
SELECT fromReadableSize('1.00 KiB');    -- 1024
SELECT fromReadableSize('3.00 KiB');    -- 3072

-- Infix whitespace is ignored
SELECT fromReadableSize('1    KiB');
SELECT fromReadableSize('1KiB');

-- Can parse LowCardinality
SELECT fromReadableSize(toLowCardinality('1 KiB'));

-- Can parse nullable fields
SELECT fromReadableSize(toNullable('1 KiB'));

-- Can parse non-const columns fields
SELECT fromReadableSize(materialize('1 KiB'));

-- Output is NULL if NULL arg is passed
SELECT fromReadableSize(NULL);

-- ERRORS
-- No arguments
SELECT fromReadableSize(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- Too many arguments
SELECT fromReadableSize('1 B', '2 B'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- Wrong Type
SELECT fromReadableSize(12); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- Invalid input - overall garbage
SELECT fromReadableSize('oh no'); -- { serverError CANNOT_PARSE_NUMBER }
-- Invalid input - unknown unit
SELECT fromReadableSize('12.3 rb'); -- { serverError CANNOT_PARSE_TEXT }
-- Invalid input - Leading whitespace
SELECT fromReadableSize(' 1 B'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
-- Invalid input - Trailing characters
SELECT fromReadableSize('1 B leftovers'); -- { serverError UNEXPECTED_DATA_AFTER_PARSED_VALUE }
-- Invalid input - Decimal size unit is not accepted
SELECT fromReadableSize('1 KB'); -- { serverError CANNOT_PARSE_TEXT }
-- Invalid input - Negative sizes are not allowed
SELECT fromReadableSize('-1 KiB'); -- { serverError BAD_ARGUMENTS }
-- Invalid input - Input too large to fit in UInt64
SELECT fromReadableSize('1000 EiB'); -- { serverError BAD_ARGUMENTS }


-- OR NULL
-- Works as the regular version when inputs are correct
SELECT
    arrayJoin(['1 B', '1 KiB', '1 MiB', '1 GiB', '1 TiB', '1 PiB', '1 EiB']) AS readable_sizes,
    fromReadableSizeOrNull(readable_sizes) AS filesize;

-- Returns NULL on invalid values
SELECT
    arrayJoin(['invalid', '1 Joe', '1KB', ' 1 GiB', '1 TiB with fries']) AS readable_sizes,
    fromReadableSizeOrNull(readable_sizes) AS filesize;


-- OR ZERO
-- Works as the regular version when inputs are correct
SELECT
    arrayJoin(['1 B', '1 KiB', '1 MiB', '1 GiB', '1 TiB', '1 PiB', '1 EiB']) AS readable_sizes,
    fromReadableSizeOrZero(readable_sizes) AS filesize;

-- Returns NULL on invalid values
SELECT
    arrayJoin(['invalid', '1 Joe', '1KB', ' 1 GiB', '1 TiB with fries']) AS readable_sizes,
    fromReadableSizeOrZero(readable_sizes) AS filesize;

