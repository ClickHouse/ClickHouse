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
SELECT formatReadableDecimalSize(fromReadableSize('1 mb'));

-- Should be able to parse decimals
SELECT fromReadableSize('1.00 KiB');    -- 1024
SELECT fromReadableSize('3.00 KiB');    -- 3072

-- Should be able to parse negative numbers
SELECT fromReadableSize('-1.00 KiB');    -- 1024

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
SELECT fromReadableSize('oh no'); -- { serverError BAD_ARGUMENTS }
-- Invalid input - unknown unit
SELECT fromReadableSize('12.3 rb'); -- { serverError BAD_ARGUMENTS }
-- Invalid input - Leading whitespace
SELECT fromReadableSize(' 1 B'); -- { serverError BAD_ARGUMENTS }
-- Invalid input - Trailing characters
SELECT fromReadableSize('1 B leftovers'); -- { serverError BAD_ARGUMENTS }
SELECT fromReadableSize(' 1 B'); -- { serverError BAD_ARGUMENTS }
