-- https://github.com/ClickHouse/ClickHouse/issues/44153
SET enable_analyzer=1;
DROP TABLE IF EXISTS parent;
DROP TABLE IF EXISTS join_table_1;
DROP TABLE IF EXISTS join_table_2;

CREATE TABLE parent(
    a_id Int64,
    b_id Int64,
    c_id Int64,
    created_at Int64
)
ENGINE=MergeTree()
ORDER BY (a_id, b_id, c_id, created_at);

CREATE TABLE join_table_1(
    a_id Int64,
    b_id Int64
)
ENGINE=MergeTree()
ORDER BY (a_id, b_id);

CREATE TABLE join_table_2(
    c_id Int64,
    created_at Int64
)
ENGINE=MergeTree()
ORDER BY (c_id, created_at);

WITH with_table as (
    SELECT p.a_id, p.b_id, p.c_id FROM parent p
    LEFT JOIN join_table_1 jt1 ON jt1.a_id = p.a_id AND jt1.b_id = p.b_id
    LEFT JOIN join_table_2 jt2 ON jt2.c_id = p.c_id
    WHERE
        p.a_id = 0 AND (jt2.c_id = 0 OR p.created_at = 0)
)
SELECT p.a_id, p.b_id, COUNT(*) as f_count FROM with_table
GROUP BY p.a_id, p.b_id;

DROP TABLE IF EXISTS parent;
DROP TABLE IF EXISTS join_table_1;
DROP TABLE IF EXISTS join_table_2;
