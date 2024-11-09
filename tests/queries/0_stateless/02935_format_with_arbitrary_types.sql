
-- Tags: no-fasttest
-- no-fasttest: json type needs rapidjson library, geo types need s2 geometry

SET allow_experimental_object_type = 1;
SET allow_experimental_json_type = 1;
SET allow_suspicious_low_cardinality_types=1;

SELECT '-- Const string + non-const arbitrary type';
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(42 :: Int8));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(43 :: Int16));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(44 :: Int32));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(45 :: Int64));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(46 :: Int128));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(47 :: Int256));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(48 :: UInt8));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(49 :: UInt16));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(50 :: UInt32));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(51 :: UInt64));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(52 :: UInt128));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(53 :: UInt256));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(42.42 :: Float32));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(43.43 :: Float64));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(44.44 :: Decimal(2)));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(true :: Bool));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(false :: Bool));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('foo' :: String));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('bar' :: FixedString(3)));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('foo' :: Nullable(String)));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('bar' :: Nullable(FixedString(3))));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('foo' :: LowCardinality(String)));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('bar' :: LowCardinality(FixedString(3))));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('foo' :: LowCardinality(Nullable(String))));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('bar' :: LowCardinality(Nullable(FixedString(3)))));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(42 :: LowCardinality(Nullable(UInt32))));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(42 :: LowCardinality(UInt32)));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('fae310ca-d52a-4923-9e9b-02bf67f4b009' :: UUID));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('2023-11-14' :: Date));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('2123-11-14' :: Date32));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('2023-11-14 05:50:12' :: DateTime('Europe/Amsterdam')));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('2023-11-14 05:50:12.123' :: DateTime64(3, 'Europe/Amsterdam')));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('hallo' :: Enum('hallo' = 1)));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(['foo', 'bar'] :: Array(String)));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('{"foo": "bar"}' :: Object('json')));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('{"foo": "bar"}' :: JSON));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize((42, 'foo') :: Tuple(Int32, String)));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize(map(42, 'foo') :: Map(Int32, String)));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('122.233.64.201' :: IPv4));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize('2001:0001:130F:0002:0003:09C0:876A:130B' :: IPv6));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize((42, 43) :: Point));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize([(0,0),(10,0),(10,10),(0,10)] :: Ring));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize([[(20, 20), (50, 20), (50, 50), (20, 50)], [(30, 30), (50, 50), (50, 30)]] :: Polygon));
SELECT format('The {0} to all questions is {1}.', 'answer', materialize([[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (50, 20), (50, 50), (20, 50)],[(30, 30), (50, 50), (50, 30)]]] :: MultiPolygon));

SELECT '-- Nested';
DROP TABLE IF EXISTS format_nested;
CREATE TABLE format_nested(attrs Nested(k String, v String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO format_nested VALUES (['foo', 'bar'], ['qaz', 'qux']);
SELECT format('The {0} to all questions is {1}.', attrs.k, attrs.v) FROM format_nested;
DROP TABLE format_nested;

SELECT '-- NULL arguments';
SELECT format('The {0} to all questions is {1}', NULL, NULL);
SELECT format('The {0} to all questions is {1}', NULL, materialize(NULL :: Nullable(UInt64)));
SELECT format('The {0} to all questions is {1}', materialize(NULL :: Nullable(UInt64)), materialize(NULL :: Nullable(UInt64)));
SELECT format('The {0} to all questions is {1}', 42, materialize(NULL :: Nullable(UInt64)));
SELECT format('The {0} to all questions is {1}', '42', materialize(NULL :: Nullable(UInt64)));
SELECT format('The {0} to all questions is {1}', 42, materialize(NULL :: Nullable(UInt64)), materialize(NULL :: Nullable(UInt64)));
SELECT format('The {0} to all questions is {1}', '42', materialize(NULL :: Nullable(UInt64)), materialize(NULL :: Nullable(UInt64)));

SELECT '-- Various arguments tests';
SELECT format('The {0} to all questions is {1}', materialize('Non-const'), materialize(' strings'));
SELECT format('The {0} to all questions is {1}', 'Two arguments ', 'test');
SELECT format('The {0} to all questions is {1} and {2}', 'Three ', 'arguments', ' test');
SELECT format('The {0} to all questions is {1} and {2}', materialize(3 :: Int64), ' arguments test', ' with int type');
SELECT format('The {0} to all questions is {1}', materialize(42 :: Int32), materialize(144 :: UInt64));
SELECT format('The {0} to all questions is {1} and {2}', materialize(42 :: Int32), materialize(144 :: UInt64), materialize(255 :: UInt32));
SELECT format('The {0} to all questions is {1}', 42, 144);
SELECT format('The {0} to all questions is {1} and {2}', 42, 144, 255);

SELECT '-- Single argument tests';
SELECT format('The answer to all questions is {0}.', 42);
SELECT format('The answer to all questions is {0}.', materialize(42));
SELECT format('The answer to all questions is {0}.', 'foo');
SELECT format('The answer to all questions is {0}.', materialize('foo'));
SELECT format('The answer to all questions is {0}.', NULL);
SELECT format('The answer to all questions is {0}.', materialize(NULL :: Nullable(UInt64)));
