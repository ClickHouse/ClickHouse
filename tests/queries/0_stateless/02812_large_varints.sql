-- 64-bit integers with MSB set (i.e. values > (1ULL<<63) - 1) could for historical/compat reasons not be serialized as var-ints (issue #51486).
-- These two queries internally produce such big values, run them to be sure no bad things happen.
SELECT topKWeightedState(65535)(now(), -2) FORMAT Null;
SELECT number FROM numbers(toUInt64(-1)) limit 10 Format Null;
