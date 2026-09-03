-- Should be the inverse of formatReadableSize
SELECT formatReadableSize(parseReadableSize('1 B'));
SELECT formatReadableSize(parseReadableSize('1 KiB'));
SELECT formatReadableSize(parseReadableSize('1 MiB'));
SELECT formatReadableSize(parseReadableSize('1 GiB'));
SELECT formatReadableSize(parseReadableSize('1 TiB'));
SELECT formatReadableSize(parseReadableSize('1 PiB'));
SELECT formatReadableSize(parseReadableSize('1 EiB'));

-- Should be the inverse of formatReadableDecimalSize
SELECT formatReadableDecimalSize(parseReadableSize('1 B'));
SELECT formatReadableDecimalSize(parseReadableSize('1 KB'));
SELECT formatReadableDecimalSize(parseReadableSize('1 MB'));
SELECT formatReadableDecimalSize(parseReadableSize('1 GB'));
SELECT formatReadableDecimalSize(parseReadableSize('1 TB'));
SELECT formatReadableDecimalSize(parseReadableSize('1 PB'));
SELECT formatReadableDecimalSize(parseReadableSize('1 EB'));

-- Is case-insensitive
SELECT formatReadableSize(parseReadableSize('1 mIb'));

-- Should be able to parse decimals
SELECT parseReadableSize('1.00 KiB');    -- 1024
SELECT parseReadableSize('3.00 KiB');    -- 3072

-- Infix whitespace is ignored
SELECT parseReadableSize('1    KiB');
SELECT parseReadableSize('1KiB');

-- Can parse LowCardinality
SELECT parseReadableSize(toLowCardinality('1 KiB'));

-- Can parse nullable fields
SELECT parseReadableSize(toNullable('1 KiB'));

-- Can parse non-const columns fields
SELECT parseReadableSize(materialize('1 KiB'));

-- Output is NULL if NULL arg is passed
SELECT parseReadableSize(NULL);

-- Can parse more decimal places than Float64's precision
SELECT parseReadableSize('3.14159265358979323846264338327950288419716939937510 KiB');

-- Can parse sizes prefixed with a plus sign
SELECT parseReadableSize('+3.1415 KiB');

-- Can parse amounts in scientific notation
SELECT parseReadableSize('10e2 B');

-- Can parse floats with no decimal points
SELECT parseReadableSize('5. B');

-- Can parse numbers with leading zeroes
SELECT parseReadableSize('002 KiB');

-- Can parse octal-like
SELECT parseReadableSize('08 KiB');

-- Can parse various flavours of zero
SELECT parseReadableSize('0 KiB'), parseReadableSize('+0 KiB'), parseReadableSize('-0 KiB');

-- ERRORS
-- No arguments
SELECT parseReadableSize(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- Too many arguments
SELECT parseReadableSize('1 B', '2 B'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- Wrong Type
SELECT parseReadableSize(12); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- Invalid input - overall garbage
SELECT parseReadableSize('oh no'); -- { serverError CANNOT_PARSE_NUMBER }
-- Invalid input - unknown unit
SELECT parseReadableSize('12.3 rb'); -- { serverError CANNOT_PARSE_TEXT }
-- Invalid input - Leading whitespace
SELECT parseReadableSize(' 1 B'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
-- Invalid input - Trailing characters
SELECT parseReadableSize('1 B leftovers'); -- { serverError UNEXPECTED_DATA_AFTER_PARSED_VALUE }
-- Invalid input - Negative sizes are not allowed
SELECT parseReadableSize('-1 KiB'); -- { serverError BAD_ARGUMENTS }
-- Invalid input - Input too large to fit in UInt64
SELECT parseReadableSize('1000 EiB'); -- { serverError BAD_ARGUMENTS }
-- Invalid input - Hexadecimal is not supported
SELECT parseReadableSize('0xa123 KiB'); -- { serverError CANNOT_PARSE_TEXT }
-- Invalid input - NaN is not supported, with or without sign and with different capitalizations
SELECT parseReadableSize('nan KiB'); -- { serverError BAD_ARGUMENTS }
SELECT parseReadableSize('+nan KiB'); -- { serverError BAD_ARGUMENTS }
SELECT parseReadableSize('-nan KiB'); -- { serverError BAD_ARGUMENTS }
SELECT parseReadableSize('NaN KiB'); -- { serverError BAD_ARGUMENTS }
-- Invalid input - Infinite is not supported, with or without sign, in all its forms
SELECT parseReadableSize('inf KiB'); -- { serverError BAD_ARGUMENTS }
SELECT parseReadableSize('+inf KiB'); -- { serverError BAD_ARGUMENTS }
SELECT parseReadableSize('-inf KiB'); -- { serverError BAD_ARGUMENTS }
SELECT parseReadableSize('infinite KiB'); -- { serverError BAD_ARGUMENTS }
SELECT parseReadableSize('+infinite KiB'); -- { serverError BAD_ARGUMENTS }
SELECT parseReadableSize('-infinite KiB'); -- { serverError BAD_ARGUMENTS }
SELECT parseReadableSize('Inf KiB'); -- { serverError BAD_ARGUMENTS }
SELECT parseReadableSize('Infinite KiB'); -- { serverError BAD_ARGUMENTS }


-- OR NULL
-- Works as the regular version when inputs are correct
SELECT
    arrayJoin(['1 B', '1 KiB', '1 MiB', '1 GiB', '1 TiB', '1 PiB', '1 EiB']) AS readable_sizes,
    parseReadableSizeOrNull(readable_sizes) AS filesize;

-- Returns NULL on invalid values
SELECT
    arrayJoin(['invalid', '1 Joe', '1KB', ' 1 GiB', '1 TiB with fries', 'NaN KiB', 'Inf KiB', '0xa123 KiB']) AS readable_sizes,
    parseReadableSizeOrNull(readable_sizes) AS filesize;


-- OR ZERO
-- Works as the regular version when inputs are correct
SELECT
    arrayJoin(['1 B', '1 KiB', '1 MiB', '1 GiB', '1 TiB', '1 PiB', '1 EiB']) AS readable_sizes,
    parseReadableSizeOrZero(readable_sizes) AS filesize;

-- Returns NULL on invalid values
SELECT
    arrayJoin(['invalid', '1 Joe', '1KB', ' 1 GiB', '1 TiB with fries', 'NaN KiB', 'Inf KiB', '0xa123 KiB']) AS readable_sizes,
    parseReadableSizeOrZero(readable_sizes) AS filesize;