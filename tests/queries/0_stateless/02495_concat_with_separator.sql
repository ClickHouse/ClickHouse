SET allow_suspicious_low_cardinality_types=1;

-- negative tests
SELECT concatWithSeparator(materialize('|'), 'a', 'b'); -- { serverError ILLEGAL_COLUMN }
SELECT concatWithSeparator();                           -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- special cases
SELECT concatWithSeparator('|') = '';
SELECT concatWithSeparator('|', 'a') == 'a';

SELECT concatWithSeparator('|', 'a', 'b') == 'a|b';
SELECT concatWithSeparator('|', 'a', materialize('b')) == 'a|b';
SELECT concatWithSeparator('|', materialize('a'), 'b') == 'a|b';
SELECT concatWithSeparator('|', materialize('a'), materialize('b')) == 'a|b';

SELECT concatWithSeparator('|', 'a', toFixedString('b', 1)) == 'a|b';
SELECT concatWithSeparator('|', 'a', materialize(toFixedString('b', 1))) == 'a|b';
SELECT concatWithSeparator('|', materialize('a'), toFixedString('b', 1)) == 'a|b';
SELECT concatWithSeparator('|', materialize('a'), materialize(toFixedString('b', 1))) == 'a|b';

SELECT concatWithSeparator('|', toFixedString('a', 1), 'b') == 'a|b';
SELECT concatWithSeparator('|', toFixedString('a', 1), materialize('b')) == 'a|b';
SELECT concatWithSeparator('|', materialize(toFixedString('a', 1)), 'b') == 'a|b';
SELECT concatWithSeparator('|', materialize(toFixedString('a', 1)), materialize('b')) == 'a|b';

SELECT concatWithSeparator('|', toFixedString('a', 1), toFixedString('b', 1)) == 'a|b';
SELECT concatWithSeparator('|', toFixedString('a', 1), materialize(toFixedString('b', 1))) == 'a|b';
SELECT concatWithSeparator('|', materialize(toFixedString('a', 1)), toFixedString('b', 1)) == 'a|b';
SELECT concatWithSeparator('|', materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1))) == 'a|b';

SELECT concatWithSeparator(null, 'a', 'b') == null;
SELECT concatWithSeparator('1', null, 'b') == null;
SELECT concatWithSeparator('1', 'a', null) == null;

-- Const String + non-const non-String/non-FixedString type'
SELECT concatWithSeparator('|', 'a', materialize(42 :: Int8)) == 'a|42';
SELECT concatWithSeparator('|', 'a', materialize(43 :: Int16)) == 'a|43';
SELECT concatWithSeparator('|', 'a', materialize(44 :: Int32)) == 'a|44';
SELECT concatWithSeparator('|', 'a', materialize(45 :: Int64)) == 'a|45';
SELECT concatWithSeparator('|', 'a', materialize(46 :: Int128)) == 'a|46';
SELECT concatWithSeparator('|', 'a', materialize(47 :: Int256)) == 'a|47';
SELECT concatWithSeparator('|', 'a', materialize(48 :: UInt8)) == 'a|48';
SELECT concatWithSeparator('|', 'a', materialize(49 :: UInt16)) == 'a|49';
SELECT concatWithSeparator('|', 'a', materialize(50 :: UInt32)) == 'a|50';
SELECT concatWithSeparator('|', 'a', materialize(51 :: UInt64)) == 'a|51';
SELECT concatWithSeparator('|', 'a', materialize(52 :: UInt128)) == 'a|52';
SELECT concatWithSeparator('|', 'a', materialize(53 :: UInt256)) == 'a|53';
SELECT concatWithSeparator('|', 'a', materialize(42.42 :: Float32)) == 'a|42.42';
SELECT concatWithSeparator('|', 'a', materialize(43.43 :: Float64)) == 'a|43.43';
SELECT concatWithSeparator('|', 'a', materialize(44.44 :: Decimal(2))) == 'a|44';
SELECT concatWithSeparator('|', 'a', materialize(true :: Bool)) == 'a|true';
SELECT concatWithSeparator('|', 'a', materialize(false :: Bool)) == 'a|false';
SELECT concatWithSeparator('|', 'a', materialize('foo' :: String)) == 'a|foo';
SELECT concatWithSeparator('|', 'a', materialize('bar' :: FixedString(3))) == 'a|bar';
SELECT concatWithSeparator('|', 'a', materialize('foo' :: Nullable(String))) == 'a|foo';
SELECT concatWithSeparator('|', 'a', materialize('bar' :: Nullable(FixedString(3)))) == 'a|bar';
SELECT concatWithSeparator('|', 'a', materialize('foo' :: LowCardinality(String))) == 'a|foo';
SELECT concatWithSeparator('|', 'a', materialize('bar' :: LowCardinality(FixedString(3)))) == 'a|bar';
SELECT concatWithSeparator('|', 'a', materialize('foo' :: LowCardinality(Nullable(String)))) == 'a|foo';
SELECT concatWithSeparator('|', 'a', materialize('bar' :: LowCardinality(Nullable(FixedString(3))))) == 'a|bar';
SELECT concatWithSeparator('|', 'a', materialize(42 :: LowCardinality(Nullable(UInt32)))) == 'a|42';
SELECT concatWithSeparator('|', 'a', materialize(42 :: LowCardinality(UInt32))) == 'a|42';
SELECT concatWithSeparator('|', 'a', materialize('fae310ca-d52a-4923-9e9b-02bf67f4b009' :: UUID)) == 'a|fae310ca-d52a-4923-9e9b-02bf67f4b009';
SELECT concatWithSeparator('|', 'a', materialize('2023-11-14' :: Date)) == 'a|2023-11-14';
SELECT concatWithSeparator('|', 'a', materialize('2123-11-14' :: Date32)) == 'a|2123-11-14';
SELECT concatWithSeparator('|', 'a', materialize('2023-11-14 05:50:12' :: DateTime('Europe/Amsterdam'))) == 'a|2023-11-14 05:50:12';
SELECT concatWithSeparator('|', 'a', materialize('hallo' :: Enum('hallo' = 1))) == 'a|hallo';
SELECT concatWithSeparator('|', 'a', materialize(['foo', 'bar'] :: Array(String))) == 'a|[\'foo\',\'bar\']';
SELECT concatWithSeparator('|', 'a', materialize((42, 'foo') :: Tuple(Int32, String))) == 'a|(42,\'foo\')';
SELECT concatWithSeparator('|', 'a', materialize(map(42, 'foo') :: Map(Int32, String))) == 'a|{42:\'foo\'}';
SELECT concatWithSeparator('|', 'a', materialize('122.233.64.201' :: IPv4)) == 'a|122.233.64.201';
SELECT concatWithSeparator('|', 'a', materialize('2001:0001:130F:0002:0003:09C0:876A:130B' :: IPv6)) == 'a|2001:0001:130F:0002:0003:09C0:876A:130B';
