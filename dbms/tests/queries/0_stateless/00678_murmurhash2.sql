SELECT murmurHash2_32(123456);
SELECT murmurHash2_32(CAST(3 AS UInt8));
SELECT murmurHash2_32(CAST(1.2684 AS Float32));
SELECT murmurHash2_32(CAST(-154477 AS Int64));
SELECT murmurHash2_32('foo');
SELECT murmurHash2_32(CAST('bar' AS FixedString(3)));
SELECT murmurHash2_32(x) FROM (SELECT CAST(1 AS Enum8('a' = 1, 'b' = 2)) as x);


