-- Tags: no-fasttest
-- no-fasttest: json type needs rapidjson library, geo types need s2 geometry

SET enable_json_type = 1;
SET allow_suspicious_low_cardinality_types=1;

SELECT '-- Const string + non-const arbitrary type';
SELECT concat('With ', materialize(42 :: Int8));
SELECT concat('With ', materialize(43 :: Int16));
SELECT concat('With ', materialize(44 :: Int32));
SELECT concat('With ', materialize(45 :: Int64));
SELECT concat('With ', materialize(46 :: Int128));
SELECT concat('With ', materialize(47 :: Int256));
SELECT concat('With ', materialize(48 :: UInt8));
SELECT concat('With ', materialize(49 :: UInt16));
SELECT concat('With ', materialize(50 :: UInt32));
SELECT concat('With ', materialize(51 :: UInt64));
SELECT concat('With ', materialize(52 :: UInt128));
SELECT concat('With ', materialize(53 :: UInt256));
SELECT concat('With ', materialize(42.42 :: Float32));
SELECT concat('With ', materialize(43.43 :: Float64));
SELECT concat('With ', materialize(44.44 :: Decimal(2)));
SELECT concat('With ', materialize(true :: Bool));
SELECT concat('With ', materialize(false :: Bool));
SELECT concat('With ', materialize('foo' :: String));
SELECT concat('With ', materialize('bar' :: FixedString(3)));
SELECT concat('With ', materialize('foo' :: Nullable(String)));
SELECT concat('With ', materialize('bar' :: Nullable(FixedString(3))));
SELECT concat('With ', materialize('foo' :: LowCardinality(String)));
SELECT concat('With ', materialize('bar' :: LowCardinality(FixedString(3))));
SELECT concat('With ', materialize('foo' :: LowCardinality(Nullable(String))));
SELECT concat('With ', materialize('bar' :: LowCardinality(Nullable(FixedString(3)))));
SELECT concat('With ', materialize(42 :: LowCardinality(Nullable(UInt32))));
SELECT concat('With ', materialize(42 :: LowCardinality(UInt32)));
SELECT concat('With ', materialize('fae310ca-d52a-4923-9e9b-02bf67f4b009' :: UUID));
SELECT concat('With ', materialize('2023-11-14' :: Date));
SELECT concat('With ', materialize('2123-11-14' :: Date32));
SELECT concat('With ', materialize('2023-11-14 05:50:12' :: DateTime('Europe/Amsterdam')));
SELECT concat('With ', materialize('2023-11-14 05:50:12.123' :: DateTime64(3, 'Europe/Amsterdam')));
SELECT concat('With ', materialize('hallo' :: Enum('hallo' = 1)));
SELECT concat('With ', materialize(['foo', 'bar'] :: Array(String)));
SELECT concat('With ', materialize('{"foo": "bar"}' :: JSON));
SELECT concat('With ', materialize((42, 'foo') :: Tuple(Int32, String)));
SELECT concat('With ', materialize(map(42, 'foo') :: Map(Int32, String)));
SELECT concat('With ', materialize('122.233.64.201' :: IPv4));
SELECT concat('With ', materialize('2001:0001:130F:0002:0003:09C0:876A:130B' :: IPv6));
SELECT concat('With ', materialize((42, 43) :: Point));
SELECT concat('With ', materialize([(0,0),(10,0),(10,10),(0,10)] :: Ring));
SELECT concat('With ', materialize([[(20, 20), (50, 20), (50, 50), (20, 50)], [(30, 30), (50, 50), (50, 30)]] :: Polygon));
SELECT concat('With ', materialize([[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (50, 20), (50, 50), (20, 50)],[(30, 30), (50, 50), (50, 30)]]] :: MultiPolygon));

SELECT '-- SimpleAggregateFunction';
DROP TABLE IF EXISTS concat_saf_test;
CREATE TABLE concat_saf_test(x SimpleAggregateFunction(max, Int32)) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO concat_saf_test VALUES (42);
INSERT INTO concat_saf_test SELECT max(number) FROM numbers(5);
SELECT concat('With ', x) FROM concat_saf_test ORDER BY x DESC;
DROP TABLE concat_saf_test;

SELECT '-- Nested';
DROP TABLE IF EXISTS concat_nested_test;
CREATE TABLE concat_nested_test(attrs Nested(k String, v String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO concat_nested_test VALUES (['foo', 'bar'], ['qaz', 'qux']);
SELECT concat('With ', attrs.k, attrs.v) FROM concat_nested_test;
DROP TABLE concat_nested_test;

SELECT '-- NULL arguments';
SELECT concat(NULL, NULL);
SELECT concat(NULL, materialize(NULL :: Nullable(UInt64)));
SELECT concat(materialize(NULL :: Nullable(UInt64)), materialize(NULL :: Nullable(UInt64)));
SELECT concat(42, materialize(NULL :: Nullable(UInt64)));
SELECT concat('42', materialize(NULL :: Nullable(UInt64)));
SELECT concat(42, materialize(NULL :: Nullable(UInt64)), materialize(NULL :: Nullable(UInt64)));
SELECT concat('42', materialize(NULL :: Nullable(UInt64)), materialize(NULL :: Nullable(UInt64)));

SELECT '-- Various arguments tests';
SELECT concat(materialize('Non-const'), materialize(' strings'));
SELECT concat('Two arguments ', 'test');
SELECT concat('Three ', 'arguments', ' test');
SELECT concat(materialize(3 :: Int64), ' arguments test', ' with int type');
SELECT concat(materialize(42 :: Int32), materialize(144 :: UInt64));
SELECT concat(materialize(42 :: Int32), materialize(144 :: UInt64), materialize(255 :: UInt32));
SELECT concat(42, 144);
SELECT concat(42, 144, 255);

SELECT '-- Single argument tests';
SELECT concat(42);
SELECT concat(materialize(42));
SELECT concat('foo');
SELECT concat(materialize('foo'));
SELECT concat(NULL);
SELECT concat(materialize(NULL :: Nullable(UInt64)));

SELECT CONCAT('Testing the ', 'alias');

SELECT '-- Empty argument tests';
SELECT concat();
select toTypeName(concat());
