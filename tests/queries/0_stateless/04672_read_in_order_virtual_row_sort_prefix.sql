-- Tags: no-parallel-replicas

-- The `ORDER BY` prefix that can be served from the sorting key is cut short at the first
-- non-strictly-monotonic function, and the virtual row optimization is only built for a prefix.
-- When the virtual row is not built, read-in-order is refused for `INNER JOIN` altogether.

DROP TABLE IF EXISTS ev;
DROP TABLE IF EXISTS dict;
DROP TABLE IF EXISTS by_day;

CREATE TABLE ev (name String, code String, ref String) ENGINE = MergeTree ORDER BY (name, code);
CREATE TABLE dict (ref String, label String) ENGINE = MergeTree ORDER BY ref;
CREATE TABLE by_day (d Date, num UInt32) ENGINE = MergeTree ORDER BY (d, num);

INSERT INTO ev VALUES ('n1', 'c1', 'r1'), ('n1', 'c2', 'r2'), ('n2', 'c1', 'r1');
INSERT INTO ev VALUES ('n2', 'c2', 'r3'), ('n3', 'c1', 'r2'), ('n3', 'c2', 'r3');
INSERT INTO dict VALUES ('r1', 'l1'), ('r2', 'l2'), ('r3', 'l3');
INSERT INTO by_day VALUES ('2020-01-01', 1), ('2020-01-01', 2), ('2020-01-02', 1);

SET optimize_read_in_order = 1, read_in_order_use_virtual_row = 1;
SET max_bytes_ratio_before_external_join = 0, max_bytes_before_external_join = 0, query_plan_read_in_order_through_join = 1;
SET query_plan_optimize_join_order_limit = 1, query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 0;

SELECT '--- single table: sorting key (name, code)';

SELECT '-- plain key';
SELECT extract(explain, '(?:Prefix sort description|Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT * FROM ev ORDER BY name, code LIMIT 5) WHERE e != '';

SELECT '-- cast on the only column';
SELECT extract(explain, '(?:Prefix sort description|Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT * FROM ev ORDER BY name::Nullable(String) LIMIT 5) WHERE e != '';

SELECT '-- cast on the first of two columns';
SELECT extract(explain, '(?:Prefix sort description|Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT * FROM ev ORDER BY name::Nullable(String), code LIMIT 5) WHERE e != '';

SELECT '-- cast on the last of two columns';
SELECT extract(explain, '(?:Prefix sort description|Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT * FROM ev ORDER BY name, code::Nullable(String) LIMIT 5) WHERE e != '';

SELECT '-- toString on the first of two columns';
SELECT extract(explain, '(?:Prefix sort description|Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT * FROM ev ORDER BY toString(name), code LIMIT 5) WHERE e != '';

SELECT '-- non-key column last';
SELECT extract(explain, '(?:Prefix sort description|Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT * FROM ev ORDER BY name, code, ref LIMIT 5) WHERE e != '';

SELECT '--- non-strictly monotonic function: the prefix is cut after it';
SELECT extract(explain, '(?:Prefix sort description|Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0 SELECT * FROM by_day ORDER BY toStartOfMonth(d), num LIMIT 5) WHERE e != '';

SELECT '--- INNER JOIN: read-in-order needs the virtual row';

SELECT '-- cast on the first of two key columns';
SELECT extract(explain, '(?:Prefix sort description|Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0
    SELECT ev.name, ev.code, dict.label FROM ev INNER JOIN dict ON ev.ref = dict.ref
    ORDER BY ev.name::Nullable(String), ev.code LIMIT 5) WHERE e != '';

SELECT '-- right table column last';
SELECT extract(explain, '(?:Prefix sort description|Read type|Virtual row conversions).*') AS e
FROM (EXPLAIN PLAN actions = 1, indexes = 0
    SELECT ev.name, ev.code, dict.label FROM ev INNER JOIN dict ON ev.ref = dict.ref
    ORDER BY ev.name, ev.code, dict.label LIMIT 5) WHERE e != '';

SELECT '--- results stay correctly ordered';

SELECT groupArray(t) = arraySort(x -> x, groupArray(t)) FROM (
    SELECT (name::Nullable(String), code) AS t FROM ev ORDER BY name::Nullable(String), code LIMIT 10);

SELECT groupArray(t) = arraySort(x -> x, groupArray(t)) FROM (
    SELECT (ev.name, ev.code, dict.label) AS t FROM ev INNER JOIN dict ON ev.ref = dict.ref
    ORDER BY ev.name, ev.code, dict.label LIMIT 10);

DROP TABLE ev;
DROP TABLE dict;
DROP TABLE by_day;
