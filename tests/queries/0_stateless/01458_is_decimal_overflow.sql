SELECT isDecimalOverflow(toDecimal32(0, 0), 0),
       isDecimalOverflow(toDecimal64(0, 0), 0),
       isDecimalOverflow(toDecimal128(0, 0), 0);

SELECT isDecimalOverflow(toDecimal32(1000000000, 0), 9),
       isDecimalOverflow(toDecimal32(1000000000, 0)),
       isDecimalOverflow(toDecimal32(-1000000000, 0), 9),
       isDecimalOverflow(toDecimal32(-1000000000, 0));
SELECT isDecimalOverflow(toDecimal32(999999999, 0), 9),
       isDecimalOverflow(toDecimal32(999999999, 0)),
       isDecimalOverflow(toDecimal32(-999999999, 0), 9),
       isDecimalOverflow(toDecimal32(-999999999, 0));
SELECT isDecimalOverflow(toDecimal32(999999999, 0), 8),
       isDecimalOverflow(toDecimal32(10, 0), 1),
       isDecimalOverflow(toDecimal32(1, 0), 0),
       isDecimalOverflow(toDecimal32(-999999999, 0), 8),
       isDecimalOverflow(toDecimal32(-10, 0), 1),
       isDecimalOverflow(toDecimal32(-1, 0), 0);

SELECT isDecimalOverflow(materialize(toDecimal32(1000000000, 0)), 9),
       isDecimalOverflow(materialize(toDecimal32(1000000000, 0))),
       isDecimalOverflow(materialize(toDecimal32(-1000000000, 0)), 9),
       isDecimalOverflow(materialize(toDecimal32(-1000000000, 0)));
SELECT isDecimalOverflow(materialize(toDecimal32(999999999, 0)), 9),
       isDecimalOverflow(materialize(toDecimal32(999999999, 0))),
       isDecimalOverflow(materialize(toDecimal32(-999999999, 0)), 9),
       isDecimalOverflow(materialize(toDecimal32(-999999999, 0)));
SELECT isDecimalOverflow(materialize(toDecimal32(999999999, 0)), 8),
       isDecimalOverflow(materialize(toDecimal32(10, 0)), 1),
       isDecimalOverflow(materialize(toDecimal32(1, 0)), 0),
       isDecimalOverflow(materialize(toDecimal32(-999999999, 0)), 8),
       isDecimalOverflow(materialize(toDecimal32(-10, 0)), 1),
       isDecimalOverflow(materialize(toDecimal32(-1, 0)), 0);

SELECT isDecimalOverflow(toDecimal64(1000000000000000000, 0), 18),
       isDecimalOverflow(toDecimal64(1000000000000000000, 0)),
       isDecimalOverflow(toDecimal64(-1000000000000000000, 0), 18),
       isDecimalOverflow(toDecimal64(-1000000000000000000, 0));
SELECT isDecimalOverflow(toDecimal64(999999999999999999, 0), 18),
       isDecimalOverflow(toDecimal64(999999999999999999, 0)),
       isDecimalOverflow(toDecimal64(-999999999999999999, 0), 18),
       isDecimalOverflow(toDecimal64(-999999999999999999, 0));
SELECT isDecimalOverflow(toDecimal64(999999999999999999, 0), 17),
       isDecimalOverflow(toDecimal64(10, 0), 1),
       isDecimalOverflow(toDecimal64(1, 0), 0),
       isDecimalOverflow(toDecimal64(-999999999999999999, 0), 17),
       isDecimalOverflow(toDecimal64(-10, 0), 1),
       isDecimalOverflow(toDecimal64(-1, 0), 0);

SELECT isDecimalOverflow(materialize(toDecimal64(1000000000000000000, 0)), 18),
       isDecimalOverflow(materialize(toDecimal64(1000000000000000000, 0))),
       isDecimalOverflow(materialize(toDecimal64(-1000000000000000000, 0)), 18),
       isDecimalOverflow(materialize(toDecimal64(-1000000000000000000, 0)));
SELECT isDecimalOverflow(materialize(toDecimal64(999999999999999999, 0)), 18),
       isDecimalOverflow(materialize(toDecimal64(999999999999999999, 0))),
       isDecimalOverflow(materialize(toDecimal64(-999999999999999999, 0)), 18),
       isDecimalOverflow(materialize(toDecimal64(-999999999999999999, 0)));
SELECT isDecimalOverflow(materialize(toDecimal64(999999999999999999, 0)), 17),
       isDecimalOverflow(materialize(toDecimal64(10, 0)), 1),
       isDecimalOverflow(materialize(toDecimal64(1, 0)), 0),
       isDecimalOverflow(materialize(toDecimal64(-999999999999999999, 0)), 17),
       isDecimalOverflow(materialize(toDecimal64(-10, 0)), 1),
       isDecimalOverflow(materialize(toDecimal64(-1, 0)), 0);

SELECT isDecimalOverflow(toDecimal128('99999999999999999999999999999999999999', 0) + 1, 38),
       isDecimalOverflow(toDecimal128('99999999999999999999999999999999999999', 0) + 1),
       isDecimalOverflow(toDecimal128('-99999999999999999999999999999999999999', 0) - 1, 38),
       isDecimalOverflow(toDecimal128('-99999999999999999999999999999999999999', 0) - 1);
SELECT isDecimalOverflow(toDecimal128('99999999999999999999999999999999999999', 0), 38),
       isDecimalOverflow(toDecimal128('99999999999999999999999999999999999999', 0)),
       isDecimalOverflow(toDecimal128('-99999999999999999999999999999999999999', 0), 38),
       isDecimalOverflow(toDecimal128('-99999999999999999999999999999999999999', 0));
SELECT isDecimalOverflow(toDecimal128('99999999999999999999999999999999999999', 0), 37),
       isDecimalOverflow(toDecimal128('10', 0), 1),
       isDecimalOverflow(toDecimal128('1', 0), 0),
       isDecimalOverflow(toDecimal128('-99999999999999999999999999999999999999', 0), 37),
       isDecimalOverflow(toDecimal128('-10', 0), 1),
       isDecimalOverflow(toDecimal128('-1', 0), 0);

SELECT isDecimalOverflow(materialize(toDecimal128('99999999999999999999999999999999999999', 0)) + 1, 38),
       isDecimalOverflow(materialize(toDecimal128('99999999999999999999999999999999999999', 0)) + 1),
       isDecimalOverflow(materialize(toDecimal128('-99999999999999999999999999999999999999', 0)) - 1, 38),
       isDecimalOverflow(materialize(toDecimal128('-99999999999999999999999999999999999999', 0)) - 1);
SELECT isDecimalOverflow(materialize(toDecimal128('99999999999999999999999999999999999999', 0)), 38),
       isDecimalOverflow(materialize(toDecimal128('99999999999999999999999999999999999999', 0))),
       isDecimalOverflow(materialize(toDecimal128('-99999999999999999999999999999999999999', 0)), 38),
       isDecimalOverflow(materialize(toDecimal128('-99999999999999999999999999999999999999', 0)));
SELECT isDecimalOverflow(materialize(toDecimal128('99999999999999999999999999999999999999', 0)), 37),
       isDecimalOverflow(materialize(toDecimal128('10', 0)), 1),
       isDecimalOverflow(materialize(toDecimal128('1', 0)), 0),
       isDecimalOverflow(materialize(toDecimal128('-99999999999999999999999999999999999999', 0)), 37),
       isDecimalOverflow(materialize(toDecimal128('-10', 0)), 1),
       isDecimalOverflow(materialize(toDecimal128('-1', 0)), 0);

SELECT isDecimalOverflow(toNullable(toDecimal32(42, 0)), 1),
       isDecimalOverflow(materialize(toNullable(toDecimal32(42, 0))), 2),
       isDecimalOverflow(toNullable(toDecimal64(42, 0)), 1),
       isDecimalOverflow(materialize(toNullable(toDecimal64(42, 0))), 2),
       isDecimalOverflow(toNullable(toDecimal128(42, 0)), 1),
       isDecimalOverflow(materialize(toNullable(toDecimal128(42, 0))), 2);
