SELECT materialize(toDecimal64(4,4)) - materialize(toDecimal32(2,2));
SELECT toDecimal64(4,4) - materialize(toDecimal32(2,2));
SELECT materialize(toDecimal64(4,4)) - toDecimal32(2,2);
SELECT toDecimal64(4,4) - toDecimal32(2,2);
