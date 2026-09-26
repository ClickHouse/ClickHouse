-- Query plan serialization round-trips LimitRangeStep. The serializer writes the AFTER (start)
-- condition before the UNTIL (end) condition; the deserializer must read them in the same wire
-- order. A query with both AFTER and UNTIL proves the predicates are not swapped/misdecoded.

-- { echo }

SELECT number FROM numbers(20) ORDER BY number LIMIT 5 AFTER number >= 3 UNTIL number >= 15 SETTINGS serialize_query_plan = 1;
SELECT number FROM numbers(20) ORDER BY number LIMIT AFTER number >= 3 UNTIL number >= 8 SETTINGS serialize_query_plan = 1;
SELECT number FROM numbers(20) ORDER BY number LIMIT 3 AFTER number >= 10 SETTINGS serialize_query_plan = 1;
SELECT number FROM numbers(20) ORDER BY number LIMIT UNTIL number >= 4 SETTINGS serialize_query_plan = 1;
SELECT number FROM numbers(20) ORDER BY number LIMIT 2 AFTER number IN (3, 9) ALL UNTIL number >= 12 SETTINGS serialize_query_plan = 1;
