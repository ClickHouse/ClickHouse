-- A `FixedString` needle compares equal to a `String` value both with and without its trailing zero
-- bytes, and a term-preserving tokenizer stores those two spellings as two different terms. One index
-- lookup cannot cover both, so the index has to decline the atom instead of pruning the granule that
-- holds the other spelling.

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS t_text_padded_needle;

CREATE TABLE t_text_padded_needle (id UInt32, s String, INDEX tix s TYPE text(tokenizer = array) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_text_padded_needle SELECT 1, concat('hello', unhex('0000000000'));
INSERT INTO t_text_padded_needle SELECT 2, 'world';
INSERT INTO t_text_padded_needle SELECT 3, 'hello';

SELECT 'both spellings equal the needle';
SELECT concat('hello', unhex('0000000000')) = toFixedString('hello', 10), 'hello' = toFixedString('hello', 10);

SELECT 'the same rows with and without the index';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('hello', 10) ORDER BY id);

SELECT 'a needle that carries no padding still prunes';
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = toFixedString('world', 5) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE s = 'hello' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_text_padded_needle WHERE hasAny(splitByString(',', s), [toFixedString('world', 5)]) ORDER BY id);

DROP TABLE t_text_padded_needle;
