DROP TABLE IF EXISTS merge_tree;
CREATE TABLE merge_tree (d Date) ENGINE = MergeTree ORDER BY d PARTITION BY d;

INSERT INTO merge_tree VALUES ('2020-01-01'), ('2020-01-02'), ('2020-01-03'), ('2020-01-04'), ('2020-01-05'), ('2020-01-06');
SELECT 1, * FROM merge_tree ORDER BY d;

-- ALTER TABLE merge_tree DROP PARTITION 2020-01-02; -- This does not even parse
-- SELECT 2, * FROM merge_tree;

ALTER TABLE merge_tree DROP PARTITION 20200103; -- unfortunately, this works, but not as user expected.
SELECT 3, * FROM merge_tree ORDER BY d;

ALTER TABLE merge_tree DROP PARTITION '20200104';
SELECT 4, * FROM merge_tree ORDER BY d;

ALTER TABLE merge_tree DROP PARTITION '2020-01-05';
SELECT 5, * FROM merge_tree ORDER BY d;

ALTER TABLE merge_tree DROP PARTITION '202001-06'; -- { serverError 38 }
SELECT 6, * FROM merge_tree ORDER BY d;

DROP TABLE merge_tree;
