DROP TABLE IF EXISTS 3538_array_except1;
DROP TABLE IF EXISTS 3538_array_except2;
DROP TABLE IF EXISTS 3538_array_except3;
DROP TABLE IF EXISTS 3538_array_except4;
DROP TABLE IF EXISTS 3538_array_except5;
DROP TABLE IF EXISTS 3538_array_except6;

SELECT arrayExcept([1, 2, 3, 4], [3, 5]) AS result;
SELECT arrayExcept([1, 2, 2, 3], [2]) AS result;
SELECT arrayExcept(['apple', 'banana', 'cherry'], ['banana', 'date']) AS result;

SELECT arrayExcept([]::Array(UInt8), [1, 2]) AS result;
SELECT arrayExcept([1, 2, 3], []::Array(UInt8)) AS result;
SELECT arrayExcept([1, 2, 3], [1, 2, 3]) AS result;

SELECT arrayExcept(['laptop', 'phone', 'tablet', 'watch'], ['watch', 'headphones']) AS result;
SELECT arrayExcept([200, 404, 404, 500, 503], [404, 500]) AS result;

SELECT arrayExcept([1, NULL, 2], [2]) AS result;
SELECT arrayExcept([1, 2, 3], [2, NULL]) AS result;

SELECT arrayExcept(materialize(['11','2','3','4','0']), materialize([1.5])); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayExcept(materialize('11'), materialize('1')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayExcept(materialize(['11','2','3','4','0']), materialize('1')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayExcept(materialize(['11','2','3','4','0']), materialize('1')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayExcept(materialize([['11','2','3','4','0']]), materialize([['1']])); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT arrayExcept([1, 2, 3, 4], [3, 5]) AS result FROM numbers(3);
SELECT arrayExcept(materialize([1, 2, 3, 4]), [3, 5]) AS result FROM numbers(3);

WITH excludes AS (
    SELECT 1 as id, ['b','d'] AS exclude
    UNION ALL
    SELECT 2 as id, ['a','c']
    UNION ALL
    SELECT 3 as id, ['x','y']
)
SELECT
    id, arrayExcept(['a','b','c'], exclude) AS result
FROM excludes ORDER BY id;

SELECT
    arrayExcept(
        multiIf(
            number = 0, [1, 2, 3],
            number = 1, [4, 5, 6],
            [7, 8, 9]
        ),
        [2, 5, 8]
    ) AS result
FROM numbers(3);

SELECT arrayExcept(['premium', 'active', 'new']::Array(LowCardinality(String)), ['active']::Array(LowCardinality(String))) AS result;
SELECT arrayExcept(materialize(['premium', 'active', 'new']::Array(LowCardinality(String))), ['active']::Array(LowCardinality(String))) AS result;
SELECT arrayExcept(['premium', 'active', 'new']::Array(LowCardinality(String)), materialize(['active']::Array(LowCardinality(String)))) AS result;
SELECT arrayExcept(materialize(['premium', 'active', 'new']::Array(LowCardinality(String))), materialize(['active']::Array(LowCardinality(String)))) AS result;
SELECT arrayExcept(['a','b','c']::Array(LowCardinality(String)), ['b','d']::Array(String)) AS result;

CREATE TABLE 3538_array_except1
(
    id UInt32,
    source Array(UInt32),
    except Array(UInt32),
    except_null Array(Nullable(UInt32)),
    expected Array(UInt32)
)
ENGINE = Memory;

INSERT INTO 3538_array_except1 VALUES
( 1, [1, 2, 3], [2], [NULL, 2], [1, 3]),
( 2, [1, 2, 3, 2], [2], [NULL, 2], [1, 3]),
( 3, [1, 2, 3], [4], [NULL, 4], [1, 2, 3]),
( 4, [], [1], [NULL, 1], []),
( 5, [1], [], [NULL], [1]),
( 6, [], [], [NULL], []),
( 7, [1, 2], [], [NULL], [1, 2]),
( 8, [], [1, 2], [NULL, 1, 2], []),
( 9, [1, 1, 2, 3], [1], [NULL, 1], [2, 3]),
(10, [1, 2, 2, 3], [2], [NULL, 2, NULL], [1, 3]),
(11, [1, 2, 3, 3], [3], [NULL, 3], [1, 2]),
(12, [1, 2, 3, 4, 5], [2, 4], [NULL, 2, 4], [1, 3, 5]),
(13, [10, 20, 30, 40], [10, 30], [NULL, 10, 30, NULL], [20, 40]),
(14, [100, 200], [300, 400], [NULL, 300, NULL, 400], [100, 200]),
(15, [5, 6, 7], [8, 9], [NULL, 8, 9], [5, 6, 7]),
(16, [1, 2, 3], [1, 2, 3], [NULL, 1, NULL, 2, 3], []),
(17, [42], [42], [NULL, 42], []);

SELECT 'Array(UInt32) exceptArray(UInt32)';
SELECT id, source, except, arrayExcept(source, except) AS result, expected, if(result = expected, 'OK', 'NOK') AS status FROM 3538_array_except1 ORDER BY id;
SELECT 'Array(UInt32) exceptArray(Nullable(UInt32))';
SELECT id, source, except_null, arrayExcept(source, except_null) AS result, expected, if(result = expected, 'OK', 'NOK') AS status FROM 3538_array_except1 ORDER BY id;

CREATE TABLE 3538_array_except2
(
    id UInt32,
    source_null Array(Nullable(UInt32)),
    except Array(UInt32),
    expected Array(Nullable(UInt32)),
    except_null Array(Nullable(UInt32)),
    expected_null Array(Nullable(UInt32))
)
ENGINE = Memory;

INSERT INTO 3538_array_except2 VALUES
(1,  [1, 2, 3],          [2],       [1, 3],          [NULL, 2],       [1, 3]),
(2,  [1, 2, 3, 2],       [2],       [1, 3],          [NULL, 2],       [1, 3]),
(3,  [1, NULL, 3],       [2],       [1, NULL, 3],    [NULL, 2],       [1, 3]),
(4,  [1, NULL, 3],       [1],       [NULL, 3],       [NULL, 1],       [3]),
(5,  [1, NULL, 3],       [999],     [1, NULL, 3],    [NULL],          [1, 3]),
(6,  [NULL, NULL, NULL], [999],     [NULL,NULL,NULL],[NULL, 999],     []),
(7,  [NULL, NULL, NULL], [999],     [NULL,NULL,NULL],[NULL],          []),
(8,  [],                 [1],       [],              [NULL, 1],       []),
(9,  [NULL],             [999],     [NULL],          [NULL],          []),
(10, [],                 [999],     [],              [NULL],          []),
(11, [1, 2, 3],          [1, 2, 3], [],              [NULL, 1, 2, 3], []),
(12, [NULL, NULL],       [999],     [NULL, NULL],    [NULL],          []),
(13, [1, NULL, 2, NULL, 3], [2],    [1, NULL, NULL, 3], [NULL, 2],     [1, 3]),
(14, [1, NULL, 2, 3],    [1, 3],    [NULL, 2],       [NULL, 1, 3],    [2]),
(15, [1, NULL, 3],       [4, 5],    [1, NULL, 3],    [NULL, 4, 5],    [1, 3]),
(16, [NULL, NULL],       [1, 2],    [NULL, NULL],    [NULL, 1, 2],    []),
(17, [1, 1, NULL, NULL, 2], [1],   [NULL, NULL, 2], [NULL, 1],       [2]),
(18, [1, NULL, 1, NULL], [1],       [NULL, NULL],    [NULL, 1],       []),
(19, [1, NULL, 2, 3, NULL], [2,3], [1, NULL, NULL], [NULL, 2, 3],    [1]),
(20, [10, NULL, 20, NULL, 30], [10,30], [NULL, 20, NULL], [NULL, 10, 30], [20]);

SELECT 'Array(Nullable(UInt32)) exceptArray(UInt32)';
SELECT id, source_null, except, arrayExcept(source_null, except) AS result, expected, if(result = expected, 'OK', 'NOK') AS status FROM 3538_array_except2 ORDER BY id;
SELECT 'Array(Nullable(UInt32)) exceptArray(Nullable(UInt32))';
SELECT id, source_null, except_null, arrayExcept(source_null, except_null) AS result, expected_null, if(result = expected_null, 'OK', 'NOK') AS status FROM 3538_array_except2 ORDER BY id;

CREATE TABLE 3538_array_except3
(
    id UInt32,
    source Array(String),
    except Array(String),
    except_null Array(Nullable(String)),
    expected Array(String)
)
ENGINE = Memory;

INSERT INTO 3538_array_except3 VALUES
(1,  ['apple', 'banana', 'cherry'],          ['banana'],       [NULL, 'banana'],             ['apple', 'cherry']),
(2,  ['apple', 'banana', 'cherry', 'banana'], ['banana'],       [NULL, 'banana'],             ['apple', 'cherry']),
(3,  ['', 'banana', ''],                     ['banana'],       [NULL, 'banana'],             ['', '']),
(4,  ['', '', ''],                           [''],             [NULL, ''],                   []),
(5,  ['Apple', 'apple', 'APPLE'],            ['apple'],        [NULL, 'apple'],              ['Apple', 'APPLE']),
(6,  ['app', 'apple', 'application'],        ['apple'],        [NULL, 'apple'],              ['app', 'application']),
(7,  ['café', 'naïve', 'hôtel'],            ['naïve'],        [NULL, 'naïve'],              ['café', 'hôtel']),
(8,  ['日本', '中国', '韓国'],                ['中国'],          [NULL, '中国'],                ['日本', '韓国']),
(9,  [
    'this_is_a_very_long_string_1234567890',
    'another_long_string_abcdefghijklmnop',
    'short'
], ['another_long_string_abcdefghijklmnop'], [
    NULL, 'another_long_string_abcdefghijklmnop'
], [
    'this_is_a_very_long_string_1234567890',
    'short'
]),
(10, ['a$b', 'c*d', 'e\\f'],                 ['c*d'],          [NULL, 'c*d'],                ['a$b', 'e\\f']),
(11, ['A', 'B', 'C'],                        ['A', 'B', 'C'],   [NULL, 'A', 'B', 'C'],        []),
(12, ['X', 'X', 'X'],                        ['X'],            [NULL, 'X'],                  []),
(13, ['cat', 'dog', 'fish'],                 ['bird'],         [NULL, 'bird'],               ['cat', 'dog', 'fish']),
(14, ['alpha', 'beta'],                      ['gamma'],        [NULL, 'gamma'],              ['alpha', 'beta']),
(15, ['x', 'x', 'y', 'y'],                   ['x'],            [NULL, 'x'],                  ['y', 'y']),
(16, ['a', 'b', 'a', 'b'],                   ['a', 'b'],       [NULL, 'a', 'b'],             []),
(17, ['a', 'bb', 'ccc', 'dddd'],             ['bb', 'dddd'],   [NULL, 'bb', 'dddd'],         ['a', 'ccc']),
(18, ['short', 'medium', 'longer'],          ['medium'],       [NULL, 'medium'],             ['short', 'longer']),
(19, ['pre-1', 'pre-2', 'post-1'],           ['pre-2'],        [NULL, 'pre-2'],              ['pre-1', 'post-1']),
(20, ['start', 'middle', 'end'],             ['middle'],       [NULL, 'middle'],             ['start', 'end']);


SELECT 'Array(String) exceptArray(String)';
SELECT id, source, except, arrayExcept(source, except) AS result, expected, if(result = expected, 'OK', 'NOK') AS status FROM 3538_array_except3 ORDER BY id;
SELECT 'Array(String) exceptArray(Nullable(String))';
SELECT id, source, except_null, arrayExcept(source, except_null) AS result, expected, if(result = expected, 'OK', 'NOK') AS status FROM 3538_array_except3 ORDER BY id;

CREATE TABLE 3538_array_except4
(
    id UInt32,
    source_null Array(Nullable(String)),
    except Array(String),
    expected Array(Nullable(String)),
    except_null Array(Nullable(String)),
    expected_null Array(Nullable(String))
)
ENGINE = Memory;

INSERT INTO 3538_array_except4 VALUES
(1,  ['apple', NULL, 'cherry'],          ['apple'],       [NULL, 'cherry'],             [NULL, 'apple'],             ['cherry']),
(2,  [NULL, 'banana', NULL, 'banana'],   ['banana'],      [NULL, NULL],                [NULL, 'banana'],            []),
(3,  [NULL, NULL, NULL],                 ['missing'],     [NULL, NULL, NULL],          [NULL, 'missing'],           []),
(4,  [NULL, NULL, NULL],                 [NULL],          [NULL, NULL, NULL],          [NULL],                      []),
(5,  ['', NULL, ''],                     [''],            [NULL],                      [NULL, ''],                  []),
(6,  [NULL, '', NULL],                   ['x'],           [NULL, '', NULL],            [NULL, 'x'],                 ['']),
(7,  ['Apple', NULL, 'APPLE'],           ['apple'],       ['Apple', NULL, 'APPLE'],    [NULL, 'apple'],             ['Apple', 'APPLE']),
(8,  [NULL, 'a', NULL, 'A'],             ['a'],           [NULL, NULL, 'A'],           [NULL, 'a'],                 ['A']),
(9,  ['café', NULL, 'hôtel'],            ['naïve'],       ['café', NULL, 'hôtel'],     [NULL, 'naïve'],             ['café', 'hôtel']),
(10, [NULL, '中国', NULL],                ['日本'],         [NULL, '中国', NULL],          [NULL, '日本'],               ['中国']),
(11, ['A', NULL, 'C'],                   ['A', 'C'],      [NULL],                      [NULL, 'A', 'C'],            []),
(12, [NULL, 'X', NULL],                  ['X'],           [NULL, NULL],                [NULL, 'X'],                 []),
(13, ['cat', NULL, 'fish'],              ['bird'],        ['cat', NULL, 'fish'],       [NULL, 'bird'],              ['cat', 'fish']),
(14, [NULL, 'beta'],                     ['gamma'],       [NULL, 'beta'],              [NULL, 'gamma'],             ['beta']),
(15, ['x', NULL, 'x', 'y'],              ['x'],           [NULL, 'y'],                 [NULL, 'x'],                 ['y']),
(16, [NULL, 'b', NULL, 'b'],             ['b'],           [NULL, NULL],                [NULL, 'b'],                 []),
(17, ['pre', NULL, 'post'],              ['pre'],         [NULL, 'post'],              [NULL, 'pre'],               ['post']),
(18, [NULL, 'middle', 'end'],            ['middle'],      [NULL, 'end'],               [NULL, 'middle'],            ['end']),
(19, ['keep', NULL, 'keep'],             ['missing'],     ['keep', NULL, 'keep'],      [NULL],                      ['keep', 'keep']),
(20, [NULL, 'remove', NULL],             ['remove'],      [NULL, NULL],                [NULL, 'remove'],            []);

SELECT 'Array(Nullable(String)) exceptArray(String)';
SELECT id, source_null, except, arrayExcept(source_null, except) AS result, expected, if(result = expected, 'OK', 'NOK') AS status FROM 3538_array_except4 ORDER BY id;
SELECT 'Array(Nullable(String)) exceptArray(Nullable(String))';
SELECT id, source_null, except_null, arrayExcept(source_null, except_null) AS result, expected_null, if(result = expected_null, 'OK', 'NOK') AS status FROM 3538_array_except4 ORDER BY id;

CREATE TABLE 3538_array_except5 (
    id UInt32,
    source Array(FixedString(5)),
    except Array(FixedString(5)),
    except_null Array(Nullable(FixedString(5))),
    expected Array(FixedString(5))
) ENGINE = Memory;

INSERT INTO 3538_array_except5 VALUES
(1, ['abc\0\0', 'def\0\0'], ['def\0\0'], ['def\0\0', NULL], ['abc\0\0']),
(2, ['hello', 'wor\0\0'], ['wor\0\0'],   [NULL, 'wor\0\0'], ['hello']),
(3, ['a\0\0\0\0', 'b\0\0\0\0'], ['a\0\0\0\0', 'b\0\0\0\0'], ['a\0\0\0\0', NULL, 'b\0\0\0\0'], []),
(4, ['x\0y\0\0', 'x\0\0\0\0'], ['x\0\0\0\0'], [NULL, 'x\0\0\0\0', NULL], ['x\0y\0\0']);

SELECT 'Array(FixedString(5)) exceptArray(FixedString(5))';
SELECT id, source, except, arrayExcept(source, except) AS result, expected, if(result = expected, 'OK', 'NOK') AS status FROM 3538_array_except5 ORDER BY id;
SELECT 'Array(FixedString(5)) exceptArray(Nullable(FixedString(5)))';
SELECT id, source, except_null, arrayExcept(source, except_null) AS result, expected, if(result = expected, 'OK', 'NOK') AS status FROM 3538_array_except5 ORDER BY id;


CREATE TABLE 3538_array_except6
(
    id UInt32,
    source_null Array(Nullable(FixedString(5))),
    except Array(FixedString(5)),
    expected Array(Nullable(FixedString(5))),
    except_null Array(Nullable(FixedString(5))),
    expected_null Array(Nullable(FixedString(5)))
)
ENGINE = Memory;

INSERT INTO 3538_array_except6 VALUES
(1, ['abc\0\0', NULL, 'def\0\0'], ['def\0\0'], ['abc\0\0', NULL], [NULL, 'def\0\0'], ['abc\0\0']),
(2, [NULL, NULL, NULL], ['xyz\0\0'], [NULL, NULL, NULL], [NULL, 'xyz\0\0'], []),
(3, ['\0\0\0\0\0', 'hello', NULL], ['hello'], ['\0\0\0\0\0', NULL], [NULL, 'hello'], ['\0\0\0\0\0']),
(4, ['a\0\0\0\0', 'b\0\0\0\0'], ['a\0\0\0\0', 'b\0\0\0\0'], [], [NULL, 'a\0\0\0\0', 'b\0\0\0\0'], []),
(5, ['cat\0\0', 'dog\0\0', 'fox\0\0'], ['dog\0\0'], ['cat\0\0', 'fox\0\0'], [NULL, 'dog\0\0'], ['cat\0\0', 'fox\0\0']);

SELECT 'Array(Nullable(FixedString(5))) exceptArray(FixedString(5))';
SELECT id, source_null, except, arrayExcept(source_null, except) AS result, expected, if(result = expected, 'OK', 'NOK') AS status FROM 3538_array_except6 ORDER BY id;
SELECT 'Array(Nullable(FixedString(5))) exceptArray(Nullable(FixedString(5)))';
SELECT id, source_null, except_null, arrayExcept(source_null, except_null) AS result, expected_null, if(result = expected_null, 'OK', 'NOK') AS status FROM 3538_array_except6 ORDER BY id;

DROP TABLE IF EXISTS 3538_array_except6;
DROP TABLE IF EXISTS 3538_array_except5;
DROP TABLE IF EXISTS 3538_array_except4;
DROP TABLE IF EXISTS 3538_array_except3;
DROP TABLE IF EXISTS 3538_array_except2;
DROP TABLE IF EXISTS 3538_array_except1;
