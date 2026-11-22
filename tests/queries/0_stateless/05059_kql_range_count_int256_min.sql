-- https://github.com/ClickHouse/ClickHouse/issues/117103
-- A span of exactly the `Int256` minimum with a step of -1 has 2^255 + 1 elements, which does not
-- fit in `UInt64`. The overflowing division used to make it look like an empty range instead.

SELECT kqlRangeCount(toInt256(0), toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968'), toInt256(-1)); -- { serverError BAD_ARGUMENTS }
SELECT kqlRangeCount(toInt256(0), toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968') + 1, toInt256(-1)); -- { serverError BAD_ARGUMENTS }
SELECT kqlRangeCount(toInt256(1), toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968') + 1, toInt256(-1)); -- { serverError BAD_ARGUMENTS }

-- Neighbouring inputs keep counting.
SELECT kqlRangeCount(toInt256(0), toInt256(-10), toInt256(-1));
SELECT kqlRangeCount(toInt256(0), toInt256(10), toInt256(1));
SELECT kqlRangeCount(toInt256(-10), toInt256(-10), toInt256(-1));
