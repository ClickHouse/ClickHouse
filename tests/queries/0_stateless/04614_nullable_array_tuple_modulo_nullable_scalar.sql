-- `Nested(...)` is `Array(Tuple(...))`. The nullable scalar denominator must not
-- bypass tuple-number arithmetic and fall into a `LOGICAL_ERROR` in the element
-- executor.

SELECT modulo(CAST([(-21917331, 4207018430), (455097, 4294967295), (1082092, 67)] AS Nested(e1 Int64, e2 UInt32)), CAST(307 AS Nullable(UInt32)));

SELECT modulo(CAST([([29, 1689725], '9212:2091:3bb6:726d:f49d:a294:ffee:b2e4'), ([], 'baee:cb67:3718:ed58:b69:dc2f:a83e:4fa5')] AS Nested(e1 Array(Int32), e2 IPv6)), CAST(false AS Nullable(Bool))); -- { serverError ILLEGAL_DIVISION }
