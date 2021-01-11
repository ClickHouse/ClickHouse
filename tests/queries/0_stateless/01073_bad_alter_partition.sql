DROP TABLE IF EXISTS merge_tree;
CREATE TABLE merge_tree (d Date) ENGINE = MergeTree ORDER BY d PARTITION BY d;

INSERT INTO merge_tree VALUES ('2020-01-02');
SELECT 1, * FROM merge_tree;

-- ALTER TABLE merge_tree DROP PARTITION 2020-01-02; -- This does not even parse
-- SELECT 2, * FROM merge_tree;

ALTER TABLE merge_tree DROP PARTITION 20200102;
SELECT 3, * FROM merge_tree;

ALTER TABLE merge_tree DROP PARTITION '20200102'; -- { serverError 38 }
SELECT 4, * FROM merge_tree;

ALTER TABLE merge_tree DROP PARTITION '2020-01-02';
SELECT 5, * FROM merge_tree;

DROP TABLE merge_tree;
