WITH materialize([toDecimal32(3, 2), toDecimal32(-2, 2), toDecimal32(1, 2)]) AS values
SELECT
    arrayMin(values) = toDecimal32(-2, 2),
    arrayMax(values) = toDecimal32(3, 2),
    toTypeName(arrayMin(values)) = 'Decimal(9, 2)';

WITH materialize([toDecimal64(3, 4), toDecimal64(-2, 4), toDecimal64(1, 4)]) AS values
SELECT
    arrayMin(values) = toDecimal64(-2, 4),
    arrayMax(values) = toDecimal64(3, 4),
    toTypeName(arrayMax(values)) = 'Decimal(18, 4)';

WITH materialize([toDecimal128(3, 8), toDecimal128(-2, 8), toDecimal128(1, 8)]) AS values
SELECT
    arrayMin(values) = toDecimal128(-2, 8),
    arrayMax(values) = toDecimal128(3, 8),
    toTypeName(arrayMin(values)) = 'Decimal(38, 8)';

WITH materialize([toDecimal256(3, 12), toDecimal256(-2, 12), toDecimal256(1, 12)]) AS values
SELECT
    arrayMin(values) = toDecimal256(-2, 12),
    arrayMax(values) = toDecimal256(3, 12),
    toTypeName(arrayMax(values)) = 'Decimal(76, 12)';

WITH
    [
        toDateTime64('1960-01-01 00:00:00.123', 3, 'UTC'),
        toDateTime64('2026-01-01 00:00:00.456', 3, 'UTC'),
        toDateTime64('2000-01-01 00:00:00.789', 3, 'UTC')
    ]) AS values
SELECT
    arrayMin(values) = toDateTime64('1960-01-01 00:00:00.123', 3, 'UTC'),
    arrayMax(values) = toDateTime64('2026-01-01 00:00:00.456', 3, 'UTC'),
    toTypeName(arrayMin(values)) = toTypeName(toDateTime64('2000-01-01 00:00:00.000', 3, 'UTC'));

SELECT
    arrayMin(materialize(CAST([], 'Array(Decimal64(4))'))) = toDecimal64(0, 4),
    arrayMax(materialize(CAST([], 'Array(Decimal64(4))'))) = toDecimal64(0, 4);
