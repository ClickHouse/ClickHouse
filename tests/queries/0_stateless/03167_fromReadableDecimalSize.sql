-- Should be kept in sync with 03166_fromReadableSize.sql

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

-- Can parse more decimal places than Float64's precision
SELECT fromReadableDecimalSize('3.14159265358979323846264338327950288419716939937510 KB');

-- Can parse sizes prefixed with a plus sign
SELECT fromReadableDecimalSize('+3.1415 KB');

-- Can parse amounts in scientific notation
SELECT fromReadableDecimalSize('10e2 B');

-- Can parse floats with no decimal points
SELECT fromReadableDecimalSize('5. B');

-- Can parse numbers with leading zeroes
SELECT fromReadableDecimalSize('002 KB');

-- Can parse octal-like
SELECT fromReadableDecimalSize('08 KB');

-- Can parse various flavours of zero
SELECT fromReadableDecimalSize('0 KB'), fromReadableDecimalSize('+0 KB'), fromReadableDecimalSize('-0 KB');

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
-- Invalid input - Hexadecimal is not supported
SELECT fromReadableDecimalSize('0xa123 KB'); -- { serverError CANNOT_PARSE_TEXT }
-- Invalid input - NaN is not supported, with or without sign and with different capitalizations
SELECT fromReadableDecimalSize('nan KB'); -- { serverError BAD_ARGUMENTS }
SELECT fromReadableDecimalSize('+nan KB'); -- { serverError BAD_ARGUMENTS }
SELECT fromReadableDecimalSize('-nan KB'); -- { serverError BAD_ARGUMENTS }
SELECT fromReadableDecimalSize('NaN KB'); -- { serverError BAD_ARGUMENTS }
-- Invalid input - Infinite is not supported, with or without sign, in all its forms
SELECT fromReadableDecimalSize('inf KB'); -- { serverError BAD_ARGUMENTS }
SELECT fromReadableDecimalSize('+inf KB'); -- { serverError BAD_ARGUMENTS }
SELECT fromReadableDecimalSize('-inf KB'); -- { serverError BAD_ARGUMENTS }
SELECT fromReadableDecimalSize('infinite KB'); -- { serverError BAD_ARGUMENTS }
SELECT fromReadableDecimalSize('+infinite KB'); -- { serverError BAD_ARGUMENTS }
SELECT fromReadableDecimalSize('-infinite KB'); -- { serverError BAD_ARGUMENTS }
SELECT fromReadableDecimalSize('Inf KB'); -- { serverError BAD_ARGUMENTS }
SELECT fromReadableDecimalSize('Infinite KB'); -- { serverError BAD_ARGUMENTS }


-- OR NULL
-- Works as the regular version when inputs are correct
SELECT
    arrayJoin(['1 B', '1 KB', '1 MB', '1 GB', '1 TB', '1 PB', '1 EB']) AS readable_sizes,
    fromReadableDecimalSizeOrNull(readable_sizes) AS filesize;

-- Returns NULL on invalid values
SELECT
    arrayJoin(['invalid', '1 Joe', '1 KiB', ' 1 GB', '1 TB with fries', 'NaN KB', 'Inf KB', '0xa123 KB']) AS readable_sizes,
    fromReadableDecimalSizeOrNull(readable_sizes) AS filesize;


-- OR ZERO
-- Works as the regular version when inputs are correct
SELECT
    arrayJoin(['1 B', '1 KB', '1 MB', '1 GB', '1 TB', '1 PB', '1 EB']) AS readable_sizes,
    fromReadableDecimalSizeOrZero(readable_sizes) AS filesize;

-- Returns NULL on invalid values
SELECT
    arrayJoin(['invalid', '1 Joe', '1 KiB', ' 1 GiB', '1 TiB with fries', 'NaN KB', 'Inf KB', '0xa123 KB']) AS readable_sizes,
    fromReadableDecimalSizeOrZero(readable_sizes) AS filesize;

