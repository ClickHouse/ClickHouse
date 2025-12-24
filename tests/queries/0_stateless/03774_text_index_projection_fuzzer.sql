SET enable_full_text_index = 1;
DROP TABLE IF EXISTS t0;

CREATE TABLE t0
(
    c0 Array(String),
    INDEX i0 c0 TYPE text(tokenizer = sparseGrams),
    PROJECTION p0 (SELECT c0 ORDER BY c0)
)
ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO TABLE t0 (c0) VALUES (['A']);
INSERT INTO TABLE t0 (c0) VALUES (['B']);

OPTIMIZE TABLE t0 FINAL;

DROP TABLE t0;
