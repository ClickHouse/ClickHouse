-- Regression test for a bug where shrinkStoredBlocksToFit would replace ColumnReplicated
-- objects with clones but leave raw pointers in ColumnsInfo::replicated_columns dangling.
-- When AdderNonJoined (or buildOutputFromBlocks) later dereferenced those pointers,
-- ColumnIndex::getIndexAt received a garbage size_of_type and threw LOGICAL_ERROR.

DROP TABLE IF EXISTS locations;
DROP TABLE IF EXISTS location_tags;

CREATE TABLE locations ( location_id UInt32, city_id UInt32, name String ) ENGINE = MergeTree ORDER BY location_id;
CREATE TABLE location_tags ( location_id UInt32, name String, value String ) ENGINE = MergeTree ORDER BY name;

INSERT INTO locations VALUES (1, 100, 'Location1'), (2, 100, 'Location2'), (3, 100, 'Location3'), (4, 100, 'Location4'), (5, 100, 'Location5');
INSERT INTO location_tags VALUES (1, 'zip_code', '8011'), (2, 'zip_code', '8021'), (3, 'zip_code', '8031'), (4, 'zip_code', '8041'), (5, 'zip_code', '8051');

SET query_plan_optimize_join_order_limit = 0;
SET query_plan_join_swap_table = 0;
SET join_algorithm = 'hash';
SET enable_lazy_columns_replication = 1;

-- Use a tiny max_bytes_in_join to force shrinkStoredBlocksToFit, with 'break' to avoid a size-limit exception.
-- All right-side rows (5) fit in a single block that is added before the limit is checked,
-- so the join still produces correct results.
SET max_bytes_in_join = 1;
SET join_overflow_mode = 'break';

SELECT z1.value
FROM location_tags AS z2
RIGHT JOIN (
    SELECT
        l.city_id AS lcity_id,
        z1.value AS value
    FROM location_tags AS z1
    RIGHT JOIN locations AS l ON z1.location_id = l.location_id
) z1 ON z2.location_id = lcity_id
ORDER BY 1;

DROP TABLE locations;
DROP TABLE location_tags;
