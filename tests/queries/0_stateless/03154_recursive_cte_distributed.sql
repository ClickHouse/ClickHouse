-- Tags: shard

SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id String,
    parent_id String
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO test_table VALUES ('a', '');
INSERT INTO test_table VALUES ('b', 'a');
INSERT INTO test_table VALUES ('c', 'a');

WITH RECURSIVE search_tree AS (
    SELECT id, parent_id, [parent_id] AS path, toUInt64(0) AS depth
    FROM test_table
    UNION ALL
    SELECT t.id, t.parent_id, arrayConcat(path, [t.id]) as path, depth + 1
    FROM test_table t, search_tree st
    WHERE t.parent_id = st.id)
SELECT * FROM search_tree ORDER BY depth, id, parent_id;

SELECT '--';

WITH RECURSIVE search_tree AS (
    SELECT id, parent_id, [parent_id] AS path, toUInt64(0) AS depth
    FROM remote('127.0.0.1', currentDatabase(), test_table)
    UNION ALL
    SELECT t.id, t.parent_id, arrayConcat(path, [t.id]) as path, depth + 1
    FROM remote('127.0.0.1', currentDatabase(), test_table) t, search_tree st
    WHERE t.parent_id = st.id)
SELECT * FROM search_tree ORDER BY depth, id, parent_id;

SELECT '--';

WITH RECURSIVE search_tree AS (
    SELECT id, parent_id, [parent_id] AS path, toUInt64(0) AS depth
    FROM remote('127.0.0.{1,2}', currentDatabase(), test_table)
    UNION ALL
    SELECT t.id, t.parent_id, arrayConcat(path, [t.id]) as path, depth + 1
    FROM remote('127.0.0.{1,2}', currentDatabase(), test_table) t, search_tree st
    WHERE t.parent_id = st.id)
SELECT * FROM search_tree ORDER BY depth, id, parent_id;;

DROP TABLE test_table;
