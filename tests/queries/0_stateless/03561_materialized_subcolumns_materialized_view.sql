DROP TABLE IF EXISTS source, destination, source_to_destination_mv;

CREATE OR REPLACE TABLE destination (
  some_data Nullable(JSON),
  a_val String MATERIALIZED some_data.a[1]
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE OR REPLACE TABLE source (
  some_data Nullable(JSON)
)
ENGINE = Null;

CREATE MATERIALIZED VIEW source_to_destination_mv TO destination AS
SELECT some_data
FROM source;

INSERT INTO source VALUES('{"a": ["baz"]}');

SELECT some_data, a_val FROM destination;
