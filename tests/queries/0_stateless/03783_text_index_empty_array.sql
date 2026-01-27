-- https://github.com/ClickHouse/ClickHouse/issues/93432

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS t;

CREATE TABLE t
(
  c Array(String),
  INDEX i c TYPE text(tokenizer=array)
)
ENGINE=MergeTree() ORDER BY tuple()
AS SELECT [];

SELECT * from t WHERE hasAllTokens(c, 'abc');

DROP TABLE t;
