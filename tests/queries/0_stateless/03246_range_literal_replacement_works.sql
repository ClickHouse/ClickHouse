CREATE TABLE my_table (
  str String,
) ORDER BY str;

INSERT INTO my_table VALUES
(
CASE WHEN
  (0 BETWEEN 0 AND 2) THEN 'ok' ELSE
  'wat'
END
);

SELECT * FROM my_table;