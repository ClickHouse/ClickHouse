SELECT groupArrayDistinctMerge(
    CAST(unhex('010110') AS AggregateFunction(groupArrayDistinct, Variant(UInt8, String)))
); -- { serverError INCORRECT_DATA }
