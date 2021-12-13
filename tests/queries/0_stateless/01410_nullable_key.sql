DROP TABLE IF EXISTS nullable_key;
CREATE TABLE nullable_key (k Nullable(int), v int) ENGINE MergeTree ORDER BY k SETTINGS allow_nullable_key = 1;

INSERT INTO nullable_key SELECT number * 2, number * 3 FROM numbers(10);
INSERT INTO nullable_key SELECT NULL, -number FROM numbers(3);

SELECT * FROM nullable_key ORDER BY k;
SELECT * FROM nullable_key WHERE k IS NULL;
SELECT * FROM nullable_key WHERE k IS NOT NULL;
SELECT * FROM nullable_key WHERE k > 10;
SELECT * FROM nullable_key WHERE k < 10;

DROP TABLE nullable_key;
