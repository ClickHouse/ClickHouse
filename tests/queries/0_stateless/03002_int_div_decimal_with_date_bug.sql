SELECT intDiv(CAST('1.0', 'Decimal256(3)'), today()); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT intDiv(CAST('1.0', 'Decimal256(3)'), toDate('2023-01-02')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT intDiv(CAST('1.0', 'Decimal256(2)'), toDate32('2023-01-02 12:12:12')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT intDiv(CAST('1.0', 'Decimal256(2)'), toDateTime('2023-01-02 12:12:12')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT intDiv(CAST('1.0', 'Decimal256(2)'), toDateTime64('2023-01-02 12:12:12.002', 3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
