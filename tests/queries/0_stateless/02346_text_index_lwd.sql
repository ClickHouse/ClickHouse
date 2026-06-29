-- Some tests for lightweight deleted on a column with text index

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (
    s FixedString(37),
    INDEX idx s TYPE text(tokenizer = array())
) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO TABLE tab (s) VALUES ('爸爸'), (97);

DELETE FROM tab WHERE TRUE;

INSERT INTO TABLE tab (s) VALUES (''), (''), ('see'), ('was');
INSERT INTO TABLE tab (s) VALUES ('thought'), ('认识你很高兴'), (''), ('叫'), (x'C328'), ('叫'), ('日本'), ('哪国人'), ('日本');
INSERT INTO TABLE tab (s) VALUES ('would'), ('\t'), ('need'), ('('), ('名字');
INSERT INTO TABLE tab (s) SELECT CAST(number AS String) FROM numbers(5);
INSERT INTO TABLE tab (s) SELECT s FROM generateRandom('s FixedString(37)', 12734763443271340066, 25, 2) LIMIT 3;

DELETE FROM tab WHERE TRUE;
OPTIMIZE TABLE tab FINAL;

SELECT count() FROM tab WHERE hasAllTokens(s, 'was');

DROP TABLE tab;
