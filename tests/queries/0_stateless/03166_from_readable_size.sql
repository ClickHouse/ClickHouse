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

-- Should be able to parse decimals
SELECT fromReadableSize('1.00 KiB');    -- 1024
SELECT fromReadableSize('3.00 KiB');    -- 3072

-- Resulting bytes are rounded up
SELECT fromReadableSize('1.0001 KiB');  -- 1025

-- Surrounding whitespace and trailing punctuation is ignored
SELECT fromReadableSize('   1    KiB.   ');