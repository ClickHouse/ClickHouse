-- Tags: no-fasttest
-- Tag no-fasttest: the pre-tokenizers need the Unicode character categories of ICU

-- The vocabulary named here is declared in tests/config/config.d/bpe_vocabularies.xml: every single
-- byte, so that any text can be encoded, plus a few merges, one of which is `banana`.

SELECT '-- text becomes token ids, and the ids become the text again';
SELECT tokenizeBPE('a banana', 'example_vocabulary') AS ids, detokenizeBPE(ids, 'example_vocabulary');
SELECT tokenizeBPE('hello world', 'example_vocabulary') AS ids, detokenizeBPE(ids, 'example_vocabulary');

SELECT '-- the number of tokens of a text is the length of the result';
SELECT length(tokenizeBPE('banana banana banana', 'example_vocabulary'));

SELECT '-- a token of the vocabulary is one id, and merging prefers the lowest rank';
SELECT tokenizeBPE('banana', 'example_vocabulary'), tokenizeBPE('anna', 'example_vocabulary');

SELECT '-- an empty text has no tokens';
SELECT tokenizeBPE('', 'example_vocabulary'), detokenizeBPE([], 'example_vocabulary') = '';

SELECT '-- the text is tokenized as text, so a piece of it that reads like a special token is not one';
SELECT length(tokenizeBPE('<|endoftext|>', 'example_vocabulary')) > 1;

SELECT '-- a string that is not valid UTF-8 survives the round trip';
SELECT detokenizeBPE(tokenizeBPE(unhex('ff00fe8081'), 'example_vocabulary'), 'example_vocabulary') = unhex('ff00fe8081');

SELECT '-- over a column';
CREATE TABLE prompts (id UInt32, prompt String) ENGINE = Memory;
INSERT INTO prompts VALUES (1, 'a banana'), (2, 'hello world'), (3, '');
SELECT id, length(tokenizeBPE(prompt, 'example_vocabulary')) AS tokens, detokenizeBPE(tokenizeBPE(prompt, 'example_vocabulary'), 'example_vocabulary') = prompt AS round_trips FROM prompts ORDER BY id;

SELECT '-- errors';
SELECT tokenizeBPE('a', 'no_such_vocabulary'); -- { serverError BAD_ARGUMENTS }
SELECT tokenizeBPE('a', materialize('example_vocabulary')); -- { serverError ILLEGAL_COLUMN }
SELECT detokenizeBPE([999999], 'example_vocabulary'); -- { serverError BAD_ARGUMENTS }
SELECT detokenizeBPE([toUInt64(4294967296)], 'example_vocabulary'); -- { serverError BAD_ARGUMENTS }
SELECT tokenizeBPE('a'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT tokenizeBPE(1, 'example_vocabulary'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT detokenizeBPE('a', 'example_vocabulary'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
