DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    s Array(String),
    INDEX idx s TYPE text(tokenizer = sparseGrams),
    PROJECTION p (SELECT s ORDER BY s)
)
ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO TABLE tab (s) VALUES (['A']);
INSERT INTO TABLE tab (s) VALUES (['B']);

OPTIMIZE TABLE tab FINAL;

DROP TABLE tab;
