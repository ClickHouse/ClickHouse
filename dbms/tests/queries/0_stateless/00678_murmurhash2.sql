SELECT murmurHash2(123456);
SELECT murmurHash2(CAST(3 AS UInt8));
SELECT murmurHash2(CAST(1.2684 AS Float32));
SELECT murmurHash2(CAST(-154477 AS Int64));
SELECT murmurHash2('foo');
SELECT murmurHash2(CAST('bar' AS FixedString(3)));
SELECT murmurHash2(x) FROM (SELECT CAST(1 AS Enum8('a' = 1, 'b' = 2)) as x);


