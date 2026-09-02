SELECT (toDecimal128(materialize('1'), 0), toDecimal128('2', 0)) < (toDecimal128('1', 0), toDecimal128('2', 0));
