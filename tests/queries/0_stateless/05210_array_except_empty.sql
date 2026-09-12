SELECT arrayExcept([1, NULL, 2], []::Array(Nullable(UInt8))) AS result;
SELECT arrayExcept(['a', 'b'], []::Array(String)) AS result;
SELECT arrayExcept([number, number + 1], materialize([]::Array(UInt64))) AS result FROM numbers(3);
SELECT arrayExcept([1, 2, 3], []::Array(UInt8)) AS result FROM numbers(3);
