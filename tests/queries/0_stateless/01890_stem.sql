-- Tags: no-fasttest
-- Tag no-fasttest: depends on libstemmer_c

SET allow_experimental_nlp_functions = 1;

SELECT '- Scalar inputs.';

SELECT '-- String input.';
SELECT stem('blessing', 'en');
SELECT stem('disguise', 'en');
SELECT stem('running', 'en');

SELECT stem('given', 'en');
SELECT stem('combinatorial', 'en');
SELECT stem('collection', 'en');
SELECT stem('possibility', 'en');
SELECT stem('studied', 'en');
SELECT stem('commonplace', 'en');
SELECT stem('packing', 'en');

SELECT stem('комбинаторной', 'ru');
SELECT stem('получила', 'ru');
SELECT stem('ограничена', 'ru');
SELECT stem('конечной', 'ru');
SELECT stem('максимальной', 'ru');
SELECT stem('суммарный', 'ru');
SELECT stem('стоимостью', 'ru');

SELECT stem('remplissage', 'fr');
SELECT stem('valeur', 'fr');
SELECT stem('maximiser', 'fr');
SELECT stem('dépasser', 'fr');
SELECT stem('intensivement', 'fr');
SELECT stem('étudié', 'fr');
SELECT stem('peuvent', 'fr');

SELECT '-- FixedString input produces String output.';
SELECT stem(toFixedString('blessing', 10), 'en'), toTypeName(stem(toFixedString('word', 10), 'en'));

SELECT '-- String trailing \\0 bytes are binary-safe (not trimmed, unlike FixedString padding).';
-- With the fix, snowball sees 'running\0' and the \0 prevents suffix removal, so the result differs from stem('running', 'en').
-- Without the fix (trimRight applied to String), both would return 'run'.
SELECT stem(concat('running', char(0)), 'en') != stem('running', 'en') AS zero_byte_preserved;

SELECT '-- Nullable(String) input preserves nullability.';
SELECT stem(toNullable('blessing'), 'en');
SELECT stem(toNullable(NULL), 'en');
SELECT toTypeName(stem(toNullable('word'), 'en'));

SELECT '-- Nullable(FixedString) input.';
SELECT stem(toNullable(toFixedString('blessing', 10)), 'en');
SELECT toTypeName(stem(toNullable(toFixedString('word', 10)), 'en'));

SELECT '-- LowCardinality(String) input.';
SELECT stem(toLowCardinality('blessing'), 'en');
SELECT toTypeName(stem(toLowCardinality('word'), 'en'));

SELECT '-- LowCardinality(FixedString) input.';
SELECT stem(toLowCardinality(toFixedString('blessing', 10)), 'en');

SELECT '- Array inputs.';

SELECT '-- Array(String) input.';
SELECT stem(['blessing', 'disguise', 'running'], 'en');
SELECT toTypeName(stem(['word'], 'en'));

SELECT '-- Array(FixedString) input produces Array(String) output.';
SELECT stem([toFixedString('blessing', 10), toFixedString('disguise', 10)], 'en');
SELECT toTypeName(stem([toFixedString('word', 10)], 'en'));

SELECT '-- Array(Nullable(String)) input preserves nulls.';
SELECT stem([toNullable('blessing'), NULL, toNullable('running')], 'en');
SELECT toTypeName(stem([toNullable('word')], 'en'));

SELECT '-- Array(LowCardinality(String)) input.';
SELECT stem([toLowCardinality('blessing'), toLowCardinality('running')], 'en');
SELECT toTypeName(stem([toLowCardinality('word')], 'en'));

SELECT '-- Array(Nullable(FixedString)) input.';
SELECT stem([toNullable(toFixedString('blessing', 10)), NULL, toNullable(toFixedString('running', 10))], 'en');
SELECT toTypeName(stem([toNullable(toFixedString('word', 10))], 'en'));

SELECT '- Multiple rows.';

SELECT '-- Multiple rows from a String column.';
SELECT stem(w, 'en') FROM (SELECT arrayJoin(['blessing', 'disguise', 'running']) AS w);

SELECT '-- Multiple rows from an Array(String) column.';
SELECT stem(arr, 'en') FROM (SELECT arrayJoin([['blessing', 'disguise'], ['running', 'faster']]) AS arr);

SELECT '-- Multiple rows from a Nullable(String) column.';
SELECT stem(w, 'en') FROM (SELECT arrayJoin([toNullable('blessing'), NULL, toNullable('running')]) AS w);

SELECT '- Table tests.';

SELECT '-- Table with a String column.';
CREATE TABLE stem_test_str (word String) ENGINE = MergeTree ORDER BY word;
INSERT INTO stem_test_str VALUES ('blessing'), ('disguise'), ('running'), ('faster');
SELECT stem(word, 'en') FROM stem_test_str ORDER BY word;
DROP TABLE stem_test_str;

SELECT '-- Table with a FixedString column.';
CREATE TABLE stem_test_fstr (word FixedString(15)) ENGINE = MergeTree ORDER BY word;
INSERT INTO stem_test_fstr VALUES ('blessing'), ('disguise'), ('running');
SELECT stem(word, 'en') FROM stem_test_fstr ORDER BY word;
DROP TABLE stem_test_fstr;

SELECT '-- Table with a Nullable(String) column.';
CREATE TABLE stem_test_null (word Nullable(String)) ENGINE = MergeTree ORDER BY word SETTINGS allow_nullable_key = 1;
INSERT INTO stem_test_null VALUES ('blessing'), (NULL), ('running');
SELECT stem(word, 'en') FROM stem_test_null ORDER BY word;
DROP TABLE stem_test_null;

SELECT '-- Table with an Array(String) column.';
CREATE TABLE stem_test_arr (words Array(String)) ENGINE = MergeTree ORDER BY words;
INSERT INTO stem_test_arr VALUES (['blessing', 'disguise']), (['running', 'faster']);
SELECT stem(words, 'en') FROM stem_test_arr ORDER BY words;
DROP TABLE stem_test_arr;

SELECT '-- Table with an Array(Nullable(String)) column.';
CREATE TABLE stem_test_arr_null (words Array(Nullable(String))) ENGINE = MergeTree ORDER BY words SETTINGS allow_nullable_key = 1;
INSERT INTO stem_test_arr_null VALUES (['blessing', NULL, 'running']), (['faster', NULL]);
SELECT stem(words, 'en') FROM stem_test_arr_null ORDER BY words;
DROP TABLE stem_test_arr_null;

SELECT '-- Table with a LowCardinality(String) column.';
CREATE TABLE stem_test_lc (word LowCardinality(String)) ENGINE = MergeTree ORDER BY word;
INSERT INTO stem_test_lc VALUES ('blessing'), ('disguise'), ('blessing');
SELECT stem(word, 'en') FROM stem_test_lc ORDER BY word;
DROP TABLE stem_test_lc;

SELECT '- Negative tests.';

SELECT '-- Whitespace in a String input raises BAD_ARGUMENTS.';
SELECT stem('hello world', 'en'); -- { serverError BAD_ARGUMENTS }

SELECT '-- Whitespace in an Array element raises BAD_ARGUMENTS.';
SELECT stem(['hello', 'hello world'], 'en'); -- { serverError BAD_ARGUMENTS }

SELECT '-- Whitespace in a FixedString input raises BAD_ARGUMENTS.';
SELECT stem(toFixedString('hello world', 15), 'en'); -- { serverError BAD_ARGUMENTS }

SELECT '-- Whitespace inside Array(Nullable(String)) raises BAD_ARGUMENTS.';
SELECT stem([toNullable('hello world')], 'en'); -- { serverError BAD_ARGUMENTS }

SELECT '-- Unsupported language raises ILLEGAL_TYPE_OF_ARGUMENT.';
SELECT stem('word', 'xx'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Wrong type for first argument raises ILLEGAL_TYPE_OF_ARGUMENT.';
SELECT stem(42, 'en'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Wrong type for second argument raises ILLEGAL_TYPE_OF_ARGUMENT.';
SELECT stem('word', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Tab character in a String input raises BAD_ARGUMENTS.';
SELECT stem(concat('hello', char(9), 'world'), 'en'); -- { serverError BAD_ARGUMENTS }

SELECT '-- Newline character in a String input raises BAD_ARGUMENTS.';
SELECT stem(concat('hello', char(10), 'world'), 'en'); -- { serverError BAD_ARGUMENTS }

SELECT '-- Array(UInt32) as first argument raises ILLEGAL_TYPE_OF_ARGUMENT.';
SELECT stem([1, 2, 3], 'en'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Array(Nullable(UInt32)) as first argument raises ILLEGAL_TYPE_OF_ARGUMENT.';
SELECT stem([toNullable(1)], 'en'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Array(Array(String)) as first argument raises ILLEGAL_TYPE_OF_ARGUMENT.';
SELECT stem([['hello', 'world']], 'en'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }


SELECT '-- Calling without the experimental setting raises SUPPORT_IS_DISABLED.';
SET allow_experimental_nlp_functions = 0;
SELECT stem('blessing', 'en'); -- { serverError SUPPORT_IS_DISABLED }
