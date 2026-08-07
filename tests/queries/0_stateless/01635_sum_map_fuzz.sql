SELECT finalizeAggregation(*) FROM (select initializeAggregation('sumMapState', [1, 2], [1, 2], [1, null]));

DROP TABLE IF EXISTS sum_map_overflow;
CREATE TABLE sum_map_overflow(events Array(UInt8), counts Array(UInt8)) ENGINE = Log;
SELECT [NULL], sumMapWithOverflow(events, [NULL], [[(NULL)]], counts) FROM sum_map_overflow; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
DROP TABLE sum_map_overflow;
