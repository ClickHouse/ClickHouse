SELECT groupArrayInsertAt(toString(number), number * 2) FROM (SELECT * FROM system.numbers LIMIT 10);
SELECT groupArrayInsertAt('-')(toString(number), number * 2) FROM (SELECT * FROM system.numbers LIMIT 10);
SELECT groupArrayInsertAt([123])(range(number), number * 2) FROM (SELECT * FROM system.numbers LIMIT 10);
SELECT number, groupArrayInsertAt(number, number) FROM (SELECT * FROM system.numbers LIMIT 10) GROUP BY number ORDER BY number;
SELECT k, ignore(groupArrayInsertAt(x, x)) FROM (SELECT dummy AS k, (randConstant() * 10) % 10 AS x FROM remote('127.0.0.{1,1}', system.one)) GROUP BY k ORDER BY k;
SELECT k, groupArrayInsertAt('-', 10)(toString(x), x) FROM (SELECT number AS k, number AS x FROM system.numbers LIMIT 11) GROUP BY k ORDER BY k;

-- Malformed serialized state must be rejected before it corrupts array offsets.
SELECT finalizeAggregation(materialize(CAST(unhex('04000000000000000000000100000000000000000200000000000000000300000000000000')
    AS AggregateFunction(groupArrayInsertAt(0, 3), UInt64, UInt64)))); -- { serverError TOO_LARGE_ARRAY_SIZE }

-- Reject an oversized count before trying to read missing elements.
SELECT finalizeAggregation(materialize(CAST(unhex('04')
    AS AggregateFunction(groupArrayInsertAt(0, 3), UInt64, UInt64)))); -- { serverError TOO_LARGE_ARRAY_SIZE }

-- An exact-length state remains valid, and shorter states are padded.
SELECT finalizeAggregation(materialize(CAST(unhex('03000000000000000000000100000000000000000200000000000000')
    AS AggregateFunction(groupArrayInsertAt(0, 3), UInt64, UInt64))));
SELECT finalizeAggregation(materialize(CAST(unhex('01000000000000000000')
    AS AggregateFunction(groupArrayInsertAt(9, 3), UInt64, UInt64))));

-- A declared length of zero leaves the state unbounded.
SELECT finalizeAggregation(materialize(CAST(unhex('04000000000000000000000100000000000000000200000000000000000300000000000000')
    AS AggregateFunction(groupArrayInsertAt(0, 0), UInt64, UInt64))));
