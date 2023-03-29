-- https://github.com/apache/kafka/blob/139f7709bd3f5926901a21e55043388728ccca78/clients/src/test/java/org/apache/kafka/common/utils/UtilsTest.java#L93

SELECT kafkaMurmurHash('21');
SELECT kafkaMurmurHash('foobar');
SELECT kafkaMurmurHash('a-little-bit-long-string');
SELECT kafkaMurmurHash('a-little-bit-longer-string');
SELECT kafkaMurmurHash('lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8');
