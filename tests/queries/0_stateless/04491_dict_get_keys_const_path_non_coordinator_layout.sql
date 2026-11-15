-- Regression for the constant path of `dictGetKeys` on dictionary layouts that are not read through
-- `DictionarySourceCoordinator` (for example `DIRECT` / `COMPLEX_KEY_DIRECT`). Such layouts read in a
-- single stream and do not tag their chunks with a `DictionaryBlockNumber`, so the parallel constant
-- path must not assume the tag is present: a release build would otherwise dereference a null chunk
-- info. The constant path must not crash and must agree with the vector path (the unoptimized scan).

DROP DICTIONARY IF EXISTS dict_direct;
DROP TABLE IF EXISTS src_direct;

CREATE TABLE src_direct
(
    id  UInt64,
    val String
)
ENGINE = Memory;

INSERT INTO src_direct VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'c'), (5, 'a');

CREATE DICTIONARY dict_direct
(
    id  UInt64,
    val String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src_direct'))
LAYOUT(DIRECT());

SELECT 'DIRECT: constant path';
SELECT arraySort(dictGetKeys('dict_direct', 'val', 'a'));
SELECT 'DIRECT: vector path';
SELECT arraySort(dictGetKeys('dict_direct', 'val', materialize('a')));
SELECT 'DIRECT: no match';
SELECT dictGetKeys('dict_direct', 'val', 'zzz');
SELECT 'DIRECT: constant path equals vector path for every value (must all be 1)';
SELECT dictGetKeys('dict_direct', 'val', x) = dictGetKeys('dict_direct', 'val', materialize(x)) AS consistent
FROM (SELECT arrayJoin(['a', 'b', 'c', 'zzz']) AS x);

DROP DICTIONARY dict_direct;
DROP TABLE src_direct;

DROP DICTIONARY IF EXISTS dict_complex_direct;
DROP TABLE IF EXISTS src_complex_direct;

CREATE TABLE src_complex_direct
(
    k1  UInt64,
    k2  String,
    val String
)
ENGINE = Memory;

INSERT INTO src_complex_direct VALUES (1, 'x', 'a'), (2, 'y', 'b'), (1, 'z', 'a'), (3, 'w', 'a');

CREATE DICTIONARY dict_complex_direct
(
    k1  UInt64,
    k2  String,
    val String
)
PRIMARY KEY k1, k2
SOURCE(CLICKHOUSE(TABLE 'src_complex_direct'))
LAYOUT(COMPLEX_KEY_DIRECT());

SELECT 'COMPLEX_KEY_DIRECT: constant path';
SELECT arraySort(dictGetKeys('dict_complex_direct', 'val', 'a'));
SELECT 'COMPLEX_KEY_DIRECT: vector path';
SELECT arraySort(dictGetKeys('dict_complex_direct', 'val', materialize('a')));
SELECT 'COMPLEX_KEY_DIRECT: constant path equals vector path for every value (must all be 1)';
SELECT dictGetKeys('dict_complex_direct', 'val', x) = dictGetKeys('dict_complex_direct', 'val', materialize(x)) AS consistent
FROM (SELECT arrayJoin(['a', 'b', 'zzz']) AS x);

DROP DICTIONARY dict_complex_direct;
DROP TABLE src_complex_direct;
