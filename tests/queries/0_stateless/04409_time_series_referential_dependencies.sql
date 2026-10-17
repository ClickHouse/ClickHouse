-- A TimeSeries table must register its external target tables as referential dependencies,
-- the same way a materialized view registers its external "TO" table. With
-- check_referential_table_dependencies = 1 an external target table cannot be dropped
-- while the TimeSeries table referencing it still exists.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS samples_table;
DROP TABLE IF EXISTS tags_table;
DROP TABLE IF EXISTS metrics_table;

CREATE TABLE samples_table
(
    id UInt64,
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree() ORDER BY (id, timestamp);

CREATE TABLE tags_table
(
    id UInt64,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time DateTime64(3),
    max_time DateTime64(3)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE metrics_table
(
    metric_family_name String,
    type String,
    unit String,
    help String
) ENGINE = ReplacingMergeTree ORDER BY metric_family_name;

CREATE TABLE ts ENGINE = TimeSeries
DATA samples_table TAGS tags_table METRICS metrics_table;

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'ts';

SET check_referential_table_dependencies = 1;

-- Each external target table is now a referential dependency of `ts`, so it can't be dropped.
DROP TABLE samples_table; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE tags_table; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE metrics_table; -- { serverError HAVE_DEPENDENT_OBJECTS }

-- After dropping the TimeSeries table the dependencies are gone and the target tables can be dropped.
DROP TABLE ts;
DROP TABLE samples_table;
DROP TABLE tags_table;
DROP TABLE metrics_table;

SELECT count() FROM system.tables WHERE database = currentDatabase()
    AND name IN ('ts', 'samples_table', 'tags_table', 'metrics_table');
