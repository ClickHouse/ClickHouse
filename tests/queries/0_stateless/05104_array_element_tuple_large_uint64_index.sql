-- A constant UInt64 index whose two's-complement Int64 reinterpretation is a small negative
-- number used to be taken for a valid index from the end, so the tuple null map marked the row
-- as not NULL while every tuple element was filled with its default.

-- { echoOn }

-- The affected window is the values within the array size of the UInt64 maximum.
SELECT arrayElementOrNull(materialize([(1, 'a'), (2, 'b')]), toUInt64(18446744073709551615));
SELECT arrayElementOrNull(materialize([(1, 'a'), (2, 'b')]), toUInt64(18446744073709551614));

-- Just below that window, and the remaining wrap points, were already handled correctly.
SELECT arrayElementOrNull(materialize([(1, 'a'), (2, 'b')]), toUInt64(18446744073709551613));
SELECT arrayElementOrNull(materialize([(1, 'a'), (2, 'b')]), toUInt64(9223372036854775808));
SELECT arrayElementOrNull(materialize([(1, 'a'), (2, 'b')]), toUInt64(9223372036854775807));
SELECT arrayElementOrNull(materialize([(1, 'a'), (2, 'b')]), toInt64(-9223372036854775808));

-- A non-constant index is dispatched on the column type instead of going through Field, and was
-- always correct. Both paths must agree.
SELECT arrayElementOrNull(materialize([(1, 'a'), (2, 'b')]), materialize(toUInt64(18446744073709551615)));
SELECT arrayElementOrNull(materialize([(1, 'a'), (2, 'b')]), materialize(toUInt64(18446744073709551614)));

-- The window depends on the size of each individual array, so rows of different sizes in one
-- block have to be judged independently.
SELECT arrayElementOrNull(arrayMap(x -> (x, toString(x)), range(number)), toUInt64(18446744073709551615)) FROM numbers(5);
SELECT arrayElementOrNull(arrayMap(x -> (x, toString(x)), range(number)), toUInt64(18446744073709551613)) FROM numbers(5);

-- arrayElement over an array of Nullable tuples builds the same null map. Type inference reaches
-- Array(Nullable(Tuple(...))) without any setting, and NULL is the default value of that type.
SELECT arrayElement(materialize([(1, 'a'), NULL, (2, 'b')]), toUInt64(18446744073709551615));
SELECT arrayElement(materialize([(1, 'a'), NULL, (2, 'b')]), 5);
SELECT arrayElementOrNull(materialize([(1, 'a'), NULL, (2, 'b')]), toUInt64(18446744073709551615));

-- A Nullable constant index carries the same UInt64 Field type.
SELECT arrayElementOrNull(materialize([(1, 'a'), (2, 'b')]), toNullable(toUInt64(18446744073709551615)));

-- Other tuple shapes reach the null map through the same branch.
SELECT arrayElementOrNull(materialize([((1, 2), 'a'), ((3, 4), 'b')]), toUInt64(18446744073709551615));
SELECT arrayElementOrNull(materialize(CAST([(1, 'a'), (2, 'b')] AS Array(Tuple(Int64, LowCardinality(String))))), toUInt64(18446744073709551615));
SELECT arrayElementOrNull(materialize(CAST([tuple(), tuple()] AS Array(Tuple()))), toUInt64(18446744073709551615));

-- Indexes inside the array keep working.
SELECT arrayElementOrNull(materialize([(1, 'a'), (2, 'b')]), toUInt64(2));
SELECT arrayElementOrNull(materialize([(1, 'a'), (2, 'b')]), -1);
