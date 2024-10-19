DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (`id` Nullable(String)) ENGINE = MergeTree ORDER BY id SETTINGS allow_nullable_key=1;
INSERT INTO t1 SELECT toString(number) FROM numbers(10);

CREATE TABLE t2 (`id` LowCardinality(String)) ENGINE = MergeTree ORDER BY id SETTINGS allow_nullable_key=1;
INSERT INTO t2 SELECT toString(number + 5) FROM numbers(10);

SET joined_subquery_requires_alias = 0, max_bytes_in_join = '100', join_algorithm = 'auto';
SELECT toTypeName(id), id FROM t1 FULL JOIN (SELECT * FROM t2) USING (id) ORDER BY id;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
