SELECT arrayDistinct(materialize(CAST([], 'Array(Decimal32(2))')));

SELECT arrayDistinct(materialize([
    toDecimal32('1.20', 2),
    toDecimal32('1.20', 2),
    toDecimal32('-2.30', 2)
]));

SELECT arrayDistinct(materialize([
    toDecimal64('1.2000', 4),
    toDecimal64('1.2000', 4),
    toDecimal64('-2.3000', 4)
]));

SELECT arrayDistinct(materialize([
    toDecimal128('1.20000000', 8),
    toDecimal128('1.20000000', 8),
    toDecimal128('-2.30000000', 8)
]));

SELECT arrayDistinct(materialize([
    toDecimal256('1.200000000000', 12),
    toDecimal256('1.200000000000', 12),
    toDecimal256('-2.300000000000', 12)
]));

SELECT arrayDistinct(materialize([
    toDecimal64('1.10', 2),
    NULL,
    NULL,
    toDecimal64('2.20', 2),
    toDecimal64('1.10', 2)
]));
