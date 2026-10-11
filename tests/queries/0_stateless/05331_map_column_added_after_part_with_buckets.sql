-- A Map column added by `ALTER` after a part was written has no streams in that
-- part. Reading it, including a key subcolumn, must not try to deserialize the
-- buckets info of a `with_buckets` layout that the part does not have.

DROP TABLE IF EXISTS t_map_added_after_part;
CREATE TABLE t_map_added_after_part (id UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';

INSERT INTO t_map_added_after_part SELECT number FROM numbers(10);
ALTER TABLE t_map_added_after_part ADD COLUMN m Map(String, UInt64);

SELECT sum(m['a']), count() FROM t_map_added_after_part;
SELECT m FROM t_map_added_after_part ORDER BY id LIMIT 1;

DROP TABLE t_map_added_after_part;
