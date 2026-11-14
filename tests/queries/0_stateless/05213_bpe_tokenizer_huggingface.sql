-- Tags: no-fasttest
-- Tag no-fasttest: the pre-tokenizer patterns need the Unicode character categories of ICU

-- The vocabulary named here is declared in tests/config/config.d/bpe_vocabularies.xml as a Hugging Face
-- `tokenizer.json`. The expected ids are the ones the `tokenizers` library gives for the same file.

SELECT '-- text becomes token ids, and the ids become the text again';
SELECT tokenizeBPE('a banana', 'example_huggingface_vocabulary') AS ids, detokenizeBPE(ids, 'example_huggingface_vocabulary');
SELECT tokenizeBPE('hello world', 'example_huggingface_vocabulary') AS ids, detokenizeBPE(ids, 'example_huggingface_vocabulary');
SELECT tokenizeBPE('banana banana banana', 'example_huggingface_vocabulary');

SELECT '-- pairs merge in the order of the list of merges, not in the order of the ids';
SELECT tokenizeBPE('banana', 'example_huggingface_vocabulary'), tokenizeBPE('anna', 'example_huggingface_vocabulary');

SELECT '-- the text is normalized to NFC first, so both forms of the same text give the same ids';
SELECT tokenizeBPE('caf' || char(0xC3, 0xA9), 'example_huggingface_vocabulary') = tokenizeBPE('cafe' || char(0xCC, 0x81), 'example_huggingface_vocabulary');

SELECT '-- an added token is found in the text and is one id, but a special token is text';
SELECT tokenizeBPE('<tool>banana', 'example_huggingface_vocabulary') AS ids, detokenizeBPE(ids, 'example_huggingface_vocabulary');
SELECT tokenizeBPE('<|end|>', 'example_huggingface_vocabulary');

SELECT '-- an empty text has no tokens';
SELECT tokenizeBPE('', 'example_huggingface_vocabulary');

SELECT '-- over a column';
CREATE TABLE prompts (id UInt32, prompt String) ENGINE = Memory;
INSERT INTO prompts VALUES (1, 'a banana'), (2, 'hello world'), (3, ''), (4, 'Привет, мир!');
SELECT id, length(tokenizeBPE(prompt, 'example_huggingface_vocabulary')) AS tokens, detokenizeBPE(tokenizeBPE(prompt, 'example_huggingface_vocabulary'), 'example_huggingface_vocabulary') = prompt AS round_trips FROM prompts ORDER BY id;
DROP TABLE prompts;
