SET enable_analyzer = 1;

DROP TABLE IF EXISTS tree;
CREATE TABLE tree
(
    id UInt64,
    link Nullable(UInt64),
    data String
) ENGINE=TinyLog;

INSERT INTO tree VALUES (0, NULL, 'ROOT'), (1, 0, 'Child_1'), (2, 0, 'Child_2'), (3, 1, 'Child_1_1');

WITH RECURSIVE search_tree AS (
    SELECT id, link, data
    FROM tree t
    WHERE t.id = 0
  UNION ALL
    SELECT t.id, t.link, t.data
    FROM tree t, search_tree st
    WHERE t.link = st.id
)
SELECT * FROM search_tree;

SELECT '--';

WITH RECURSIVE search_tree AS (
    SELECT id, link, data, [t.id] AS path
    FROM tree t
    WHERE t.id = 0
  UNION ALL
    SELECT t.id, t.link, t.data, arrayConcat(path, [t.id])
    FROM tree t, search_tree st
    WHERE t.link = st.id
)
SELECT * FROM search_tree;

DROP TABLE tree;
