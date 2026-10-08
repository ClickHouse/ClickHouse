-- Merging a part down to zero rows (e.g. by lightweight delete) leaves a text index whose marks
-- file holds a single mark, while the part reports zero marks. Prewarming the mark cache on attach
-- then failed to read that index with "Too many marks in file ... skp_idx_idx.cmrk2".

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (
    s FixedString(37),
    INDEX idx s TYPE text(tokenizer = array())
) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, prewarm_mark_cache = 1;

INSERT INTO TABLE tab (s) VALUES ('爸爸'), (97);

DELETE FROM tab WHERE TRUE;

INSERT INTO TABLE tab (s) VALUES (''), (''), ('see'), ('was');
INSERT INTO TABLE tab (s) VALUES ('thought'), ('认识你很高兴'), (''), ('叫'), (x'C328'), ('叫'), ('日本'), ('哪国人'), ('日本');
INSERT INTO TABLE tab (s) VALUES ('would'), ('\t'), ('need'), ('('), ('名字');
INSERT INTO TABLE tab (s) SELECT CAST(number AS String) FROM numbers(5);
INSERT INTO TABLE tab (s) SELECT s FROM generateRandom('s FixedString(37)', 12734763443271340066, 25, 2) LIMIT 3;

DELETE FROM tab WHERE TRUE;
OPTIMIZE TABLE tab FINAL;

-- Force the startup mark-cache prewarm, which reads the text index of the empty part.
DETACH TABLE tab;
ATTACH TABLE tab;

SELECT count() FROM tab WHERE hasAllTokens(s, 'was');

DROP TABLE tab;
