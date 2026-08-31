-- A block of aggregate function states is read into a single allocation of the size of a state
-- times the number of rows, and both of them come from the data, so their product can wrap around
-- and the states are then created outside of the allocated block.
-- The state below is 2^40 bytes, and the block declares 2^24 rows.

SELECT count() FROM format(Native, 'x AggregateFunction(countResampleIfResample(0, 1048576, 1, 0, 131072, 1), UInt64, UInt8, UInt64)', unhex('018080800801785E41676772656761746546756E6374696F6E28636F756E74526573616D706C654966526573616D706C6528302C20313034383537362C20312C20302C203133313037322C2031292C2055496E7436342C2055496E74382C2055496E743634290000000000000000')); -- { serverError TOO_LARGE_ARRAY_SIZE }

SELECT sumMerge(x) FROM format(Native, 'x AggregateFunction(sum, UInt64)', unhex('010101781E41676772656761746546756E6374696F6E2873756D2C2055496E743634292D00000000000000'));
