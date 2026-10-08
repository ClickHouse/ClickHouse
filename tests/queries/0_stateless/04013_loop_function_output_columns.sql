-- https://github.com/ClickHouse/ClickHouse/issues/83757
SELECT 1 FROM loop('system', 'merge_tree_settings') LIMIT 1;
