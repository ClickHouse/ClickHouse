SET enable_full_text_index = 1;

DROP TABLE IF EXISTS t_text_index_lwd;
CREATE TABLE t_text_index_lwd (c0 FixedString(37), INDEX i0 c0 TYPE text(tokenizer = array())) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO TABLE t_text_index_lwd (c0) VALUES ('爸爸'), (97);

DELETE FROM t_text_index_lwd WHERE TRUE;

INSERT INTO TABLE t_text_index_lwd (c0) VALUES (''), (''), ('see'), ('was');
INSERT INTO TABLE t_text_index_lwd (c0) VALUES ('thought'), ('认识你很高兴'), (''), ('叫'), (x'C328'), ('叫'), ('日本'), ('哪国人'), ('日本');
INSERT INTO TABLE t_text_index_lwd (c0) VALUES ('would'), ('\t'), ('need'), ('('), ('名字');
INSERT INTO TABLE t_text_index_lwd (c0) SELECT CAST(number AS String) FROM numbers(5);
INSERT INTO TABLE t_text_index_lwd (c0) SELECT c0 FROM generateRandom('c0 FixedString(37)', 12734763443271340066, 25, 2) LIMIT 3;

DELETE FROM t_text_index_lwd WHERE TRUE;
OPTIMIZE TABLE t_text_index_lwd FINAL;

SELECT count() FROM t_text_index_lwd WHERE hasAllTokens(c0, 'was');

DROP TABLE t_text_index_lwd;
