SET enable_analyzer = 1;
SET enable_named_columns_in_function_tuple = 1;

SELECT
    * APPLY finalizeAggregation
FROM
(
    WITH
        (1, 2)::Tuple(a int, b int) AS nt
    SELECT
        uniqState(nt)::AggregateFunction(uniq, Tuple(int, int)) x,
        uniqState([nt])::AggregateFunction(uniq, Array(Tuple(int, int))) y,
        uniqState(map(nt, nt))::AggregateFunction(uniq, Map(Tuple(int, int), Tuple(int, int))) z
)
FORMAT JSONEachRow;

DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS users2;
DROP TABLE IF EXISTS test_mv;

CREATE TABLE users (id UInt8, city String, name String) ENGINE=Memory;
CREATE TABLE users2 (id UInt8, city_name_uniq AggregateFunction(uniq, Tuple(String,String))) ENGINE=AggregatingMergeTree() ORDER BY (id);
CREATE MATERIALIZED VIEW test_mv TO users2 AS SELECT id, uniqState((city, name)) AS city_name_uniq FROM users GROUP BY id;

INSERT INTO users VALUES (1, 'London', 'John');
INSERT INTO users VALUES (1, 'Berlin', 'Ksenia');
INSERT INTO users VALUES (2, 'Paris', 'Alice');

SELECT id, uniqMerge(city_name_uniq) FROM users2 GROUP BY id ORDER BY id;

DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS users2;
DROP TABLE IF EXISTS test_mv;
