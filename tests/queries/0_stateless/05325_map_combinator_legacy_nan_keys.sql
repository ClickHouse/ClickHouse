-- Older versions compared floating point keys of the `-Map` combinator with `std::equal_to`,
-- so their serialized states may contain several `NaN` keys, or both `-0` and `+0`.
-- Such states must still be readable, with the duplicate entries merged.

-- Two `NaN` keys with different payloads, with counts 1 and 2.
SELECT countMapMerge(CAST(unhex('02' || '000000000000F87F' || '01' || '010000000000F87F' || '02'), 'AggregateFunction(countMap, Map(Float64, UInt8))'));

-- `-0` and `+0`, with counts 3 and 4, and the key 1 with the count 5.
SELECT countMapMerge(CAST(unhex('03' || '0000000000000080' || '03' || '0000000000000000' || '04' || '000000000000F03F' || '05'), 'AggregateFunction(countMap, Map(Float64, UInt8))'));

-- The same for `Float32` keys.
SELECT countMapMerge(CAST(unhex('02' || '0000C07F' || '01' || '0100C07F' || '02'), 'AggregateFunction(countMap, Map(Float32, UInt8))'));

-- A duplicate key of a non-floating point type is still rejected.
SELECT countMapMerge(CAST(unhex('02' || '01000000' || '01' || '01000000' || '02'), 'AggregateFunction(countMap, Map(UInt32, UInt8))')); -- { serverError INCORRECT_DATA }

-- The duplicate entry of a versioned nested function is read with the same version as the others.
-- Version 1 of the `uniq` state has an extra field, so reading it as version 0 would misparse the state.
SELECT uniqMapMerge(CAST(unhex('02' || '000000000000F87F' || '0000012CCBC234' || '010000000000F87F' || '000001E7830665'), 'AggregateFunction(1, uniqMap, Map(Float64, UInt8))'));
SELECT uniqMapMerge(CAST(unhex('02' || '000000000000F87F' || '00012CCBC234' || '010000000000F87F' || '0001E7830665'), 'AggregateFunction(0, uniqMap, Map(Float64, UInt8))'));
