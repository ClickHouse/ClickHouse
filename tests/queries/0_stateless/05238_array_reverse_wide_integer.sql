SELECT arrayReverse(materialize(CAST([1, 2, 3], 'Array(Int128)')));
SELECT arrayReverse(materialize(CAST([1, 2, 3], 'Array(UInt128)')));
SELECT arrayReverse(materialize(CAST([1, 2, 3], 'Array(Int256)')));
SELECT arrayReverse(materialize(CAST([1, 2, 3], 'Array(UInt256)')));
SELECT arrayReverse(materialize(CAST([1, NULL, 3], 'Array(Nullable(Int128))')));
