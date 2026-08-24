-- Test for Bug 90609

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    text String,
    INDEX idx text TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO tab VALUES ('Alick a01');

SELECT 1
FROM
(
  SELECT *
  FROM (
    EXPLAIN actions = 1 SELECT text FROM tab WHERE hasAnyTokens(text, ['Alick'])
    SETTINGS use_skip_indexes_on_data_read = 1
  )
)
PREWHERE
  (EXPLAIN actions = 1 SELECT 1 FROM tab PREWHERE hasAllTokens(text, ['Alick'])) = '1'; -- { serverError INCORRECT_RESULT_OF_SCALAR_SUBQUERY }

-- Another test caused the same bug.
WITH sub AS
(
  SELECT text
  FROM tab
  WHERE hasAnyTokens(text, ['Alick'])
)
SELECT *
FROM
(
  SELECT text
  FROM tab
  WHERE hasAnyTokens(text, ['Alick'])
)
WHERE (SELECT * FROM sub) != ''
SETTINGS use_skip_indexes_on_data_read = 1;

DROP TABLE tab;
