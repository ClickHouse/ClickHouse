SELECT CAST('\x01\x00' AS AggregateFunction(nothingArrayIf, Array(Nullable(Nothing)), Nullable(Nothing))); -- { serverError INCORRECT_DATA }
