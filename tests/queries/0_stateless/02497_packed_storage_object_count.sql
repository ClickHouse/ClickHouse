-- Tags: no-fasttest
-- no-fasttest: needs an S3-backed disk so the part has remote objects to count.

DROP TABLE IF EXISTS t_packed_objects SYNC;
DROP TABLE IF EXISTS t_full_objects SYNC;

-- Packed storage keeps the whole part in a single archive object (data.packed); full storage
-- stores one object per file. So a packed part has far fewer physical objects than a full one.
CREATE TABLE t_packed_objects (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 's3_cache', min_bytes_for_full_part_storage = '1G';

CREATE TABLE t_full_objects (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 's3_cache', min_bytes_for_full_part_storage = 0;

INSERT INTO t_packed_objects VALUES (1, 'foo');
INSERT INTO t_full_objects VALUES (1, 'foo');

WITH
    (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't_packed_objects') AS packed_uuid,
    (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't_full_objects') AS full_uuid,
    (SELECT count(DISTINCT local_path) FROM system.remote_data_paths WHERE local_path LIKE concat('%', toString(packed_uuid), '/all_%')) AS packed_objects,
    (SELECT count(DISTINCT local_path) FROM system.remote_data_paths WHERE local_path LIKE concat('%', toString(full_uuid), '/all_%')) AS full_objects
SELECT packed_objects, packed_objects < full_objects AS packed_has_fewer_objects;

DROP TABLE t_packed_objects SYNC;
DROP TABLE t_full_objects SYNC;
