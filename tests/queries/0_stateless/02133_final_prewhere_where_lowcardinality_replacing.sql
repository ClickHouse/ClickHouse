DROP TABLE IF EXISTS errors_local;

CREATE TABLE errors_local (level LowCardinality(String)) ENGINE=ReplacingMergeTree ORDER BY level settings min_bytes_for_wide_part = '10000000';
insert into errors_local select toString(number) from numbers(10000);

SELECT toTypeName(level) FROM errors_local FINAL PREWHERE isNotNull(level) WHERE isNotNull(level) LIMIT 1;

DROP TABLE errors_local;

CREATE TABLE errors_local(level LowCardinality(String)) ENGINE=ReplacingMergeTree ORDER BY level;
insert into errors_local select toString(number) from numbers(10000);

SELECT toTypeName(level) FROM errors_local FINAL PREWHERE isNotNull(level) WHERE isNotNull(level) LIMIT 1;

DROP TABLE errors_local;
