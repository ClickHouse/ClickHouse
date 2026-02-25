SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    str String,
    INDEX text_idx str TYPE text(tokenizer = ngrams(3)) GRANULARITY 1,
    INDEX set_idx str TYPE set(10) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS compress_marks = 0;

INSERT INTO tab (str) VALUES ('I am inverted');

OPTIMIZE TABLE tab FINAL;

-- to double check: `ll -h $(find . -name "*text_idx*")` from build dir
-- sum up .mrk* or .cmrk* files to get marks_bytes
-- sum up .idx files for data_compressed_bytes
-- note that `du` rounds to nearest 4KB so it is not accurate here
-- also note that different runs of db might all show up, only sum up one set
SELECT 
    database,
    table,
    name,
    type,
    type_full,
    granularity,
    data_compressed_bytes > 100,
    data_uncompressed_bytes > 75,
    marks_bytes
FROM system.data_skipping_indices WHERE database = currentDatabase() AND type = 'text' FORMAT Vertical;

-- to double check: `ll -h $(find . -name "*skp_idx*")` from build dir
-- see above notes
SELECT
    partition,
    name,
    secondary_indices_compressed_bytes > 150,
    secondary_indices_uncompressed_bytes > 100,
    secondary_indices_marks_bytes
FROM system.parts 
WHERE database = currentDatabase() AND table = 'tab' AND active = 1 AND partition = 'tuple()'
FORMAT Vertical;

DROP TABLE tab;
