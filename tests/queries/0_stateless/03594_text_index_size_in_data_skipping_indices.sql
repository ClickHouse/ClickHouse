SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX text_idx str TYPE text(tokenizer = 'ngram', ngram_size = 3) GRANULARITY 1,
    INDEX set_idx str TYPE set(10) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;

INSERT INTO tab (str) VALUES ('I am inverted');
INSERT INTO tab (str) VALUES ('the quick brown fox jumped over the lazy dog');
INSERT INTO tab (str) VALUES ('lorem ipsum blah blah');
INSERT INTO tab (str) VALUES ('adsj adksfjlh askjh asdkjh adskfjh asdkjh dhdkjhf  g e lsdhsdghdkl   g udgs liu sdh  ds  s g ds lgds hu gsd k');
SELECT * FROM system.data_skipping_indices WHERE database = currentDatabase();

--SELECT * FROM tab WHERE str LIKE '%dog%'

--DROP TABLE tab;

--SET allow_experimental_full_text_index = 0;
