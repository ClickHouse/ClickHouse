-- Tags: no-fasttest
-- https://github.com/ClickHouse/ClickHouse/issues/67303
SELECT uniqTheta(tuple());
SELECT uniq(tuple());
