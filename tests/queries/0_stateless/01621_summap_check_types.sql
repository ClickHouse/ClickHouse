select initializeAggregation('sumMap', [1, 2], [1, 2], [1, null]);

CREATE TEMPORARY TABLE sum_map_overflow (events Array(UInt8), counts Array(UInt8));
INSERT INTO sum_map_overflow VALUES ([1], [255]), ([1], [2]);
SELECT [NULL], sumMapWithOverflow(events, [NULL], [[(NULL)]], counts) FROM sum_map_overflow; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
