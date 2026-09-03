DROP TABLE IF EXISTS literal_alias_misclassification;

CREATE TABLE literal_alias_misclassification
(
    `id` Int64,
    `a` Nullable(String),
    `b` Nullable(Int64)
)
ENGINE = MergeTree
ORDER BY id;


INSERT INTO literal_alias_misclassification values(1, 'a', 1);
INSERT INTO literal_alias_misclassification values(2, 'b', 2);

SELECT 'const' AS r, b 
FROM
  ( SELECT a AS r, b FROM literal_alias_misclassification ) AS t1
  LEFT JOIN
  ( SELECT a AS r FROM literal_alias_misclassification ) AS t2 
  ON t1.r = t2.r
ORDER BY b;

DROP TABLE IF EXISTS literal_alias_misclassification;
