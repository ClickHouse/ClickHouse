SELECT toTypeName(sumMapFilteredState([1, 2])([1, 2, 3], [10, 10, 10]));
SELECT hex(sumMapFilteredState([1, 2])([1, 2, 3], [10, 10, 10]));
SELECT hex(unhex('02010A00000000000000020A00000000000000')::AggregateFunction(1, sumMapFiltered([1, 2]), Array(UInt8), Array(UInt8)));
SELECT sumMapFilteredMerge([1, 2])(*) FROM remote('127.0.0.{1,2}', view(SELECT sumMapFilteredState([1, 2])([1, 2, 3], [10, 10, 10])));
