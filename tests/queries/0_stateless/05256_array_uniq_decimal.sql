SELECT
    arrayUniq(materialize(CAST([], 'Array(Decimal32(2))'))),
    arrayUniq(materialize(CAST([], 'Array(Decimal64(4))'))),
    arrayUniq(materialize(CAST([], 'Array(Decimal128(8))'))),
    arrayUniq(materialize(CAST([], 'Array(Decimal256(12))')));

SELECT
    arrayUniq(materialize([toDecimal32('1.20', 2), toDecimal32('1.20', 2), toDecimal32('-2.30', 2)])),
    arrayUniq(materialize([toDecimal64('1.2000', 4), toDecimal64('1.2000', 4), toDecimal64('-2.3000', 4)])),
    arrayUniq(materialize([toDecimal128('1.20000000', 8), toDecimal128('1.20000000', 8), toDecimal128('-2.30000000', 8)])),
    arrayUniq(materialize([toDecimal256('1.200000000000', 12), toDecimal256('1.200000000000', 12), toDecimal256('-2.300000000000', 12)]));

SELECT
    arrayUniq(materialize([toDecimal32('1.20', 2), toDecimal32('1.20', 2), toDecimal32('1.20', 2)])),
    arrayUniq(materialize([toDecimal64('1.2000', 4), toDecimal64('1.2000', 4), toDecimal64('1.2000', 4)])),
    arrayUniq(materialize([toDecimal128('1.20000000', 8), toDecimal128('1.20000000', 8), toDecimal128('1.20000000', 8)])),
    arrayUniq(materialize([toDecimal256('1.200000000000', 12), toDecimal256('1.200000000000', 12), toDecimal256('1.200000000000', 12)]));

SELECT arrayUniq(materialize([
    toDecimal64('1.10', 2),
    CAST(NULL, 'Nullable(Decimal64(2))'),
    CAST(NULL, 'Nullable(Decimal64(2))'),
    toDecimal64('2.20', 2),
    toDecimal64('1.10', 2)
]));
