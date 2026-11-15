-- Zero precision must yield Time64(0), not Time, and must not drop the fractional tail.
SELECT toTypeName(toTime64OrZero('12:30:45.789', 0));
SELECT toTime64OrZero('12:30:45.789', 0);
SELECT toTypeName(toTime64OrNull('12:30:45.789', 0));
SELECT toTime64OrNull('12:30:45.789', 0);

-- The strict function already behaves this way; the tolerant variants must agree with it.
SELECT toTypeName(toTime64('12:30:45.789', 0));
SELECT toTime64('12:30:45.789', 0);

-- Non-zero precision was never affected.
SELECT toTypeName(toTime64OrZero('12:30:45.789', 3));
SELECT toTime64OrZero('12:30:45.789', 3);

-- Default precision, with no explicit argument.
SELECT toTypeName(toTime64OrZero('12:30:45.789'));
SELECT toTime64OrZero('12:30:45.789');

-- Invalid input still yields zero or NULL, at the requested precision.
SELECT toTypeName(toTime64OrZero('invalid', 0));
SELECT toTime64OrZero('invalid', 0);
SELECT toTypeName(toTime64OrNull('invalid', 0));
SELECT toTime64OrNull('invalid', 0);
