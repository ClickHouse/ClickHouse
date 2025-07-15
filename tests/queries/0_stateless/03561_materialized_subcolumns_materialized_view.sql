DROP TABLE IF EXISTS foo, foo_input, mv_foo;

CREATE OR REPLACE TABLE foo (
  some_data Nullable(JSON),
  a_val String MATERIALIZED some_data.a[1]
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE OR REPLACE TABLE foo_input (
  some_data Nullable(JSON)
)
ENGINE = Null;

CREATE MATERIALIZED VIEW mv_foo TO foo AS
SELECT some_data
FROM foo_input;

INSERT INTO foo_input VALUES('{"a": ["baz"]}');

SELECT some_data, a_val FROM foo;
