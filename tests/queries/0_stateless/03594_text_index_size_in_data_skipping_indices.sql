SET allow_experimental_full_text_index = 1;

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX text_idx str TYPE text(tokenizer = 'ngram', ngram_size = 3) GRANULARITY 1,
    INDEX set_idx str TYPE set(10) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO tab (str) VALUES ('I am inverted');
SELECT * FROM system.data_skipping_indices WHERE database = currentDatabase() FORMAT TabSeparatedWithNames;

DROP TABLE tab;

SET allow_experimental_full_text_index = 0;
