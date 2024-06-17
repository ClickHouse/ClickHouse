DROP TABLE IF EXISTS table_with_pk_clear;

CREATE TABLE table_with_pk_clear(
  key1 UInt64,
  key2 String,
  value1 String,
  value2 String
)
ENGINE = MergeTree()
ORDER by (key1, key2);

INSERT INTO table_with_pk_clear SELECT number, number * number, toString(number), toString(number * number) FROM numbers(1000);

ALTER TABLE table_with_pk_clear CLEAR COLUMN key1 IN PARTITION tuple(); --{serverError 524}

SELECT count(distinct key1) FROM table_with_pk_clear;

ALTER TABLE table_with_pk_clear CLEAR COLUMN key2 IN PARTITION tuple(); --{serverError 524}

SELECT count(distinct key2) FROM table_with_pk_clear;

DROP TABLE IF EXISTS table_with_pk_clear;
