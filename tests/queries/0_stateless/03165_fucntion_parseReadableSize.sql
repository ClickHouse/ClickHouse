-- Should be the inverse of formatReadableSize
SELECT formatReadableSize(fromReadableSize('1 B'));
SELECT formatReadableSize(fromReadableSize('1 KiB'));
SELECT formatReadableSize(fromReadableSize('1 MiB'));
SELECT formatReadableSize(fromReadableSize('1 GiB'));
SELECT formatReadableSize(fromReadableSize('1 TiB'));
SELECT formatReadableSize(fromReadableSize('1 PiB'));
SELECT formatReadableSize(fromReadableSize('1 EiB'));

-- Should be the inverse of formatReadableDecimalSize
SELECT formatReadableDecimalSize(fromReadableSize('1 B'));
SELECT formatReadableDecimalSize(fromReadableSize('1 KB'));
SELECT formatReadableDecimalSize(fromReadableSize('1 MB'));
SELECT formatReadableDecimalSize(fromReadableSize('1 GB'));
SELECT formatReadableDecimalSize(fromReadableSize('1 TB'));
SELECT formatReadableDecimalSize(fromReadableSize('1 PB'));
SELECT formatReadableDecimalSize(fromReadableSize('1 EB'));

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

-- Can parse more decimal places than Float64's precision
SELECT fromReadableSize('3.14159265358979323846264338327950288419716939937510 KiB');

-- Can parse sizes prefixed with a plus sign
SELECT fromReadableSize('+3.1415 KiB');

-- Can parse amounts in scientific notation
SELECT fromReadableSize('10e2 B');

-- Can parse floats with no decimal points
SELECT fromReadableSize('5. B');

-- Can parse numbers with leading zeroes
SELECT fromReadableSize('002 KiB');

-- Can parse octal-like
SELECT fromReadableSize('08 KiB');

-- Can parse various flavours of zero
SELECT fromReadableSize('0 KiB'), fromReadableSize('+0 KiB'), fromReadableSize('-0 KiB');

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
-- Invalid input - Negative sizes are not allowed
SELECT fromReadableSize('-1 KiB'); -- { serverError BAD_ARGUMENTS }
-- Invalid input - Input too large to fit in UInt64
SELECT fromReadableSize('1000 EiB'); -- { serverError BAD_ARGUMENTS }
-- Invalid input - Hexadecimal is not supported
SELECT fromReadableSize('0xa123 KiB'); -- { serverError CANNOT_PARSE_TEXT }
-- Invalid input - NaN is not supported, with or without sign and with different capitalizations
SELECT fromReadableSize('nan KiB'); -- { serverError BAD_ARGUMENTS }
SELECT fromReadableSize('+nan KiB'); -- { serverError BAD_ARGUMENTS }
SELECT fromReadableSize('-nan KiB'); -- { serverError BAD_ARGUMENTS }
SELECT fromReadableSize('NaN KiB'); -- { serverError BAD_ARGUMENTS }
-- Invalid input - Infinite is not supported, with or without sign, in all its forms
SELECT fromReadableSize('inf KiB'); -- { serverError BAD_ARGUMENTS }
SELECT fromReadableSize('+inf KiB'); -- { serverError BAD_ARGUMENTS }
SELECT fromReadableSize('-inf KiB'); -- { serverError BAD_ARGUMENTS }
SELECT fromReadableSize('infinite KiB'); -- { serverError BAD_ARGUMENTS }
SELECT fromReadableSize('+infinite KiB'); -- { serverError BAD_ARGUMENTS }
SELECT fromReadableSize('-infinite KiB'); -- { serverError BAD_ARGUMENTS }
SELECT fromReadableSize('Inf KiB'); -- { serverError BAD_ARGUMENTS }
SELECT fromReadableSize('Infinite KiB'); -- { serverError BAD_ARGUMENTS }


-- OR NULL
-- Works as the regular version when inputs are correct
SELECT
    arrayJoin(['1 B', '1 KiB', '1 MiB', '1 GiB', '1 TiB', '1 PiB', '1 EiB']) AS readable_sizes,
    fromReadableSizeOrNull(readable_sizes) AS filesize;

-- Returns NULL on invalid values
SELECT
    arrayJoin(['invalid', '1 Joe', '1KB', ' 1 GiB', '1 TiB with fries', 'NaN KiB', 'Inf KiB', '0xa123 KiB']) AS readable_sizes,
    fromReadableSizeOrNull(readable_sizes) AS filesize;


-- OR ZERO
-- Works as the regular version when inputs are correct
SELECT
    arrayJoin(['1 B', '1 KiB', '1 MiB', '1 GiB', '1 TiB', '1 PiB', '1 EiB']) AS readable_sizes,
    fromReadableSizeOrZero(readable_sizes) AS filesize;

-- Returns NULL on invalid values
SELECT
    arrayJoin(['invalid', '1 Joe', '1KB', ' 1 GiB', '1 TiB with fries', 'NaN KiB', 'Inf KiB', '0xa123 KiB']) AS readable_sizes,
    fromReadableSizeOrZero(readable_sizes) AS filesize;