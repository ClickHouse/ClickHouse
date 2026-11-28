-- every plan of the statement sees the same value of `randConstant`, so a projection with the table's own order reads the same marks
SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, enable_parallel_replicas = 0;

DROP TABLE IF EXISTS same_order;
CREATE TABLE same_order (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO same_order SELECT number, number % 100 FROM numbers(100000);
CREATE HYPOTHETICAL PROJECTION p_a ON same_order (SELECT a, b ORDER BY a);

SELECT if(marks[1] = marks[2], 'equal', 'different')
FROM (SELECT groupArray(toUInt64(extract(trim(explain), '\\d+'))) AS marks
      FROM (EXPLAIN WHATIF SELECT a, b FROM same_order WHERE a < randConstant() % 100000)
      WHERE startsWith(trim(explain), 'marks:'));

SELECT if(marks[1] = marks[2], 'equal', 'different')
FROM (SELECT groupArray(toUInt64(extract(trim(explain), '\\d+'))) AS marks
      FROM (EXPLAIN WHATIF SELECT a, b FROM same_order WHERE a < randConstant() % 100000)
      WHERE startsWith(trim(explain), 'marks:'));

SELECT if(marks[1] = marks[2], 'equal', 'different')
FROM (SELECT groupArray(toUInt64(extract(trim(explain), '\\d+'))) AS marks
      FROM (EXPLAIN WHATIF SELECT a, b FROM same_order WHERE a < randConstant() % 100000)
      WHERE startsWith(trim(explain), 'marks:'));

DROP TABLE same_order;
