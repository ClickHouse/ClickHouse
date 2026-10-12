DROP TABLE IF EXISTS t_granularity_zero_create;
DROP TABLE IF EXISTS t_granularity_zero_alter;
DROP TABLE IF EXISTS t_granularity_zero_create_index;

CREATE TABLE t_granularity_zero_create (a Int32, INDEX idx a TYPE minmax GRANULARITY 0) ENGINE = MergeTree ORDER BY a; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_granularity_zero_alter (a Int32) ENGINE = MergeTree ORDER BY a;
ALTER TABLE t_granularity_zero_alter ADD INDEX idx a TYPE minmax GRANULARITY 0; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_granularity_zero_create_index (a Int32) ENGINE = MergeTree ORDER BY a;
CREATE INDEX idx ON t_granularity_zero_create_index a TYPE minmax GRANULARITY 0; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_granularity_zero_alter;
DROP TABLE t_granularity_zero_create_index;
