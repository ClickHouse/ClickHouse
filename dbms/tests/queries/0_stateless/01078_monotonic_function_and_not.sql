DROP TABLE IF EXISTS table_with_monotonic_func;

CREATE TABLE table_with_monotonic_func
(
  value Float64
)
ENGINE = MergeTree
ORDER BY round(value);

INSERT INTO table_with_monotonic_func VALUES(7.42);

SELECT * FROM table_with_monotonic_func WHERE NOT (value < 7);

SELECT * FROM table_with_monotonic_func WHERE NOT NOT (value < 7); -- should produce anything

SELECT * FROM table_with_monotonic_func WHERE NOT (value > 7.43);

SELECT * FROM table_with_monotonic_func WHERE NOT (value < 7 AND value < 7.01);

SELECT * FROM table_with_monotonic_func WHERE (value < 7.44 AND value < 7.45 AND (NOT (value > 7.43)));

SELECT * FROM table_with_monotonic_func WHERE NOT (value < 7.43 and value < 7.44) OR (NOT (value < 7.01));

DROP TABLE IF EXISTS table_with_monotonic_func;
