SELECT '-- Negative tests';
SELECT arrayFold(); -- { serverError TOO_FEW_ARGUMENTS_FOR_FUNCTION }
SELECT arrayFold(1); -- { serverError TOO_FEW_ARGUMENTS_FOR_FUNCTION }
SELECT arrayFold(1, toUInt64(0)); -- { serverError TOO_FEW_ARGUMENTS_FOR_FUNCTION }
SELECT arrayFold(1, emptyArrayUInt64(), toUInt64(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayFold( acc,x -> x,  emptyArrayString(), toInt8(0)); -- { serverError TYPE_MISMATCH }
SELECT arrayFold( acc,x -> x,  'not an array', toUInt8(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayFold( acc,x,y -> x,  [0, 1], 'not an array', toUInt8(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayFold( acc,x -> x,  [0, 1], [2, 3], toUInt8(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayFold( acc,x,y -> x,  [0, 1], [2, 3, 4], toUInt8(0)); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

SELECT '-- Const arrays';
SELECT arrayFold( acc,x -> acc+x*2,  [1, 2, 3, 4], toInt64(3));
SELECT arrayFold( acc,x -> acc+x*2,  emptyArrayInt64(), toInt64(3));
SELECT arrayFold( acc,x,y -> acc+x*2+y*3,  [1, 2, 3, 4], [5, 6, 7, 8], toInt64(3));
SELECT arrayFold( acc,x -> arrayPushBack(acc, x),  [1, 2, 3, 4], emptyArrayInt64());
SELECT arrayFold( acc,x -> arrayPushFront(acc, x),  [1, 2, 3, 4], emptyArrayInt64());
SELECT arrayFold( acc,x -> (arrayPushFront(acc.1, x),arrayPushBack(acc.2, x)),  [1, 2, 3, 4], (emptyArrayInt64(), emptyArrayInt64()));
SELECT arrayFold( acc,x -> x%2 ? (arrayPushBack(acc.1, x), acc.2): (acc.1, arrayPushBack(acc.2, x)),  [1, 2, 3, 4, 5, 6], (emptyArrayInt64(), emptyArrayInt64()));

SELECT '-- Non-const arrays';
SELECT arrayFold( acc,x -> acc+x,  range(number), number) FROM system.numbers LIMIT 5;
SELECT arrayFold( acc,x -> arrayPushFront(acc,x),  range(number), emptyArrayUInt64()) FROM system.numbers LIMIT 5;
SELECT arrayFold( acc,x -> x%2 ? arrayPushFront(acc,x) : arrayPushBack(acc,x),  range(number), emptyArrayUInt64()) FROM system.numbers LIMIT 5;

SELECT '-- Bug 57458';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (line String, patterns Array(String)) ENGINE = MergeTree ORDER BY line;
INSERT INTO tab VALUES ('abcdef', ['c']), ('ghijkl', ['h', 'k']), ('mnopqr', ['n']);

SELECT
    line,
    patterns,
    arrayFold(acc, pat -> position(line, pat), patterns, 0::UInt64)
FROM tab
ORDER BY line;

DROP TABLE tab;

CREATE TABLE tab (line String) ENGINE = Memory();
INSERT INTO tab VALUES ('xxx..yyy..'), ('..........'), ('..xx..yyy.'), ('..........'), ('xxx.......');

SELECT
    line,
    splitByNonAlpha(line),
    arrayFold(
        (acc, str) -> position(line, str),
        splitByNonAlpha(line),
        0::UInt64
    )
FROM
    tab;

DROP TABLE tab;

SELECT ' -- Bug 57816';

SELECT arrayFold(acc, x -> arrayIntersect(acc, x), [['qwe', 'asd'], ['qwe','asde']], []);
