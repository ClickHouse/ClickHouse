SET allow_experimental_full_text_index = 1;

-- Issue 84805: the no-op and ngram tokenizers crash for empty inputs

SELECT 'Test no_op tokenizer';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
    str String,
    INDEX idx str TYPE text(tokenizer = 'array') )
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO TABLE tab (str) VALUES ('');

DROP TABLE tab;

SELECT 'Test ngram tokenizer';

CREATE TABLE tab (
    str String,
    INDEX idx str TYPE text(tokenizer = 'ngrams') )
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO TABLE tab (str) VALUES ('');

DROP TABLE tab;
