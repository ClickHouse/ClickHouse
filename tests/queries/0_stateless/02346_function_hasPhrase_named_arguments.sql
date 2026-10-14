-- Tests named arguments of hasPhrase: hasPhrase(input => ..., phrase => ...[, tokenizer => ...])

SELECT '-- errors';

SELECT hasPhrase(phrase => 'b', 'a b'); -- { serverError BAD_ARGUMENTS }
SELECT hasPhrase(phrase => 'b', text => 'a b'); -- { serverError BAD_ARGUMENTS }
SELECT hasPhrase(input => 'a b c', phrase => 'b c', foo => 'bar'); -- { serverError BAD_ARGUMENTS }
SELECT hasPhrase(input => 'a b c', phrase => 'b c', tokenizer => 'splitByNonAlpha', foo => 'bar'); -- { serverError BAD_ARGUMENTS }
SELECT hasPhrase('a b c', 'b c', 'splitByNonAlpha', foo => 'bar'); -- { serverError BAD_ARGUMENTS }
SELECT hasPhrase(Input => 'a b', phrase => 'b'); -- { serverError BAD_ARGUMENTS }
SELECT hasPhrase('a b', input => 'a b', phrase => 'b'); -- { serverError BAD_ARGUMENTS }
SELECT hasPhrase(phrase => 'b', phrase => 'b', input => 'a b'); -- { serverError BAD_ARGUMENTS }
SELECT hasPhrase(input => 'a b', tokenizer => 'splitByNonAlpha'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT hasPhrase('a b', 'b', 'splitByNonAlpha', 'x', phrase => 'b'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT hasPhrase(input => untuple(('a b', 'b')), phrase => 'splitByNonAlpha'); -- { serverError BAD_ARGUMENTS }
SELECT hasPhrase(untuple(('a b', 'b')), tokenizer => 'splitByNonAlpha'); -- { serverError BAD_ARGUMENTS }
SELECT hasPhrase(input => *, phrase => 'b') FROM (SELECT 'a b' AS s); -- { serverError BAD_ARGUMENTS }
SELECT hasPhrase(input => 'a b', phrase => COLUMNS('s')) FROM (SELECT 'b' AS s); -- { serverError BAD_ARGUMENTS }
WITH untuple(('a b', 'b')) AS u SELECT hasPhrase(input => u, phrase => 'splitByNonAlpha'); -- { serverError BAD_ARGUMENTS }
WITH x -> untuple(x) AS g SELECT hasPhrase(input => g(('a b', 'b')), phrase => 'splitByNonAlpha'); -- { serverError BAD_ARGUMENTS }

SELECT '-- bind';

SELECT hasPhrase(input => 'the quick brown fox', phrase => 'quick brown');
SELECT hasPhrase(phrase => 'quick brown', input => 'the quick brown fox');
SELECT hasPhrase(phrase => 'quick fox', input => 'the quick brown fox');
SELECT hasPhrase('the quick brown fox', phrase => 'brown fox');
SELECT matchPhrase(phrase => 'brown fox', input => 'the quick brown fox');
SELECT hasPhrase(tokenizer => 'array', phrase => ['quick brown'], input => ['the', 'quick brown']);
SELECT hasPhrase(['the', 'quick brown'], ['quick brown'], tokenizer => 'splitByNonAlpha');
SELECT hasPhrase(phrase => '1 x', input => concat(toString(number), ' x')) FROM numbers(3);

SELECT '-- column name';

SELECT hasPhrase(phrase => 'b c', input => 'a b c') FORMAT TSVWithNames;

SELECT '-- distributed';

SELECT hasPhrase(phrase => 'b c', input => 'a b c') FROM remote('127.0.0.{1,2}', system.one);

SELECT '-- view';

DROP VIEW IF EXISTS v_named_arguments;
CREATE VIEW v_named_arguments AS SELECT hasPhrase(phrase => 'b c', input => 'a b c') AS r;
SELECT * FROM v_named_arguments;
DROP VIEW v_named_arguments;

SELECT '-- default expression';
DROP TABLE IF EXISTS t_named_arguments;
CREATE TABLE t_named_arguments (s String, r UInt8 DEFAULT hasPhrase(phrase => 'b c', input => s)) ENGINE = Memory;
INSERT INTO t_named_arguments (s) VALUES ('a b c'), ('c b a');
SELECT * FROM t_named_arguments;
DROP TABLE t_named_arguments;

SELECT '-- text index';

DROP TABLE IF EXISTS t_named_arguments_text_index;
CREATE TABLE t_named_arguments_text_index (id UInt32, message String, INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_named_arguments_text_index VALUES (1, 'abc def foo'), (2, 'abc baz bar'), (3, 'def abc');
SELECT groupArray(id) FROM t_named_arguments_text_index WHERE hasPhrase(phrase => 'abc def', input => message)
SETTINGS force_data_skipping_indices = 'idx', use_skip_indexes = 1, use_query_condition_cache = 0;
DROP TABLE t_named_arguments_text_index;
