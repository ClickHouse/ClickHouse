SELECT 'precision = 0';
SELECT formatReadableSize(1024, 0);
SELECT formatReadableSize(1500, 0);
SELECT formatReadableSize(192851925, 0);

SELECT 'precision = 3';
SELECT formatReadableSize(1024, 3);
SELECT formatReadableSize(1500, 3);
SELECT formatReadableSize(192851925, 3);

SELECT 'precision = 7';
SELECT formatReadableSize(1024, 7);
SELECT formatReadableSize(1500, 7);
SELECT formatReadableSize(192851925, 7);

SELECT 'default precision (unchanged)';
SELECT formatReadableSize(1024);
SELECT formatReadableSize(192851925);

SELECT 'sibling functions accept precision';
SELECT formatReadableDecimalSize(1500, 0);
SELECT formatReadableDecimalSize(1500, 4);
SELECT formatReadableQuantity(1234567, 0);
SELECT formatReadableQuantity(1234567, 4);

SELECT 'errors: precision argument shape';
SELECT formatReadableSize(1024, 'two'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatReadableSize(1024, 1.5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatReadableSize(1024, -1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatReadableSize(1024, 256); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatReadableSize(1024, toUInt8(number)) FROM numbers(2); -- { serverError ILLEGAL_COLUMN }

SELECT 'errors: argument count';
SELECT formatReadableSize(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT formatReadableSize(1024, 2, 3); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
