SET check_query_single_value_result = 1;

CREATE TABLE packed_clone_source
(
    p UInt64,
    k UInt64,
    v UInt64,
    PROJECTION totals (SELECT p, sum(v) GROUP BY p)
)
ENGINE = MergeTree
PARTITION BY p ORDER BY k
SETTINGS min_bytes_for_full_part_storage = '1G', always_use_copy_instead_of_hardlinks = 1;

INSERT INTO packed_clone_source SELECT number % 2, number, number FROM numbers(40);
CREATE TABLE packed_clone_destination AS packed_clone_source;

-- Cloning the projection finalizes its writer inside the parent's shared transaction.
ALTER TABLE packed_clone_destination ATTACH PARTITION 0 FROM packed_clone_source;
SELECT p, sum(v) FROM packed_clone_destination GROUP BY p ORDER BY p
SETTINGS force_optimize_projection = 1;
CHECK TABLE packed_clone_destination;

DROP TABLE packed_clone_destination;
DROP TABLE packed_clone_source;
