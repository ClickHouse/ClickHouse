-- Tags: no-fasttest
-- no-fasttest -- compiled w/o datasketches

-- A Theta sketch state that another DataSketches implementation wrote in the compressed
-- form must deserialize to every value it retained. Both states below hold 16 distinct
-- values, so both queries must return 16. The two states pack their values with different
-- bit widths, which are decoded by separate routines.
SELECT finalizeAggregation(CAST(unhex('4B01040321011ACC9310000001F4000000FA0000007D0000003E8000001F4000000FA0000007D180000000000001F4000000FA0000007D0000003E8000001F4000000FA0000007D0000003E8') AS AggregateFunction(uniqTheta, UInt64)));
SELECT finalizeAggregation(CAST(unhex('4F01040323011ACC93100000007D0000000FA1000000004000000000000007D0000000FA0000001F40000003E80000007D0000000FA1000000000000003E80000007D0000000FA0000001F40000003E8') AS AggregateFunction(uniqTheta, UInt64)));
