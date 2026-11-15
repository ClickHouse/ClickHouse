-- Tags: no-fasttest
-- no-fasttest: needs an S3 disk.

-- A packed part on object storage is written as a single object. When the buffered data of the part exceeds
-- max_bytes_to_buffer_for_packed_part, it is moved to the temporary data of the server, not to the disk of the part,
-- so spilling must not add requests to object storage: no extra objects are written and nothing is read back.

DROP TABLE IF EXISTS t_packed_s3_spill;
DROP TABLE IF EXISTS t_packed_s3_no_spill;

-- The index granularity is fixed: with a small randomized one, the parts have many granules,
-- and their merges and mutations are too slow in debug builds with the thread fuzzer.
CREATE TABLE t_packed_s3_spill (id UInt64, s String, a Array(UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 's3_no_cache', index_granularity = 8192, index_granularity_bytes = '10Mi',
    min_bytes_for_full_part_storage = '1T', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    max_bytes_to_buffer_for_packed_part = 65536;

CREATE TABLE t_packed_s3_no_spill (id UInt64, s String, a Array(UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 's3_no_cache', index_granularity = 8192, index_granularity_bytes = '10Mi',
    min_bytes_for_full_part_storage = '1T', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    max_bytes_to_buffer_for_packed_part = 0;

SYSTEM STOP MERGES t_packed_s3_spill;
SYSTEM STOP MERGES t_packed_s3_no_spill;

INSERT INTO t_packed_s3_spill SELECT number, toString(number * 7919 % 100003), range(number % 10) FROM numbers(200000)
SETTINGS async_insert = 0, log_comment = '05367_insert_spill';
INSERT INTO t_packed_s3_no_spill SELECT number, toString(number * 7919 % 100003), range(number % 10) FROM numbers(200000)
SETTINGS async_insert = 0, log_comment = '05367_insert_no_spill';

INSERT INTO t_packed_s3_spill SELECT number, toString(number), [number] FROM numbers(200000, 100000) SETTINGS async_insert = 0;
INSERT INTO t_packed_s3_no_spill SELECT number, toString(number), [number] FROM numbers(200000, 100000) SETTINGS async_insert = 0;

SYSTEM START MERGES t_packed_s3_spill;
SYSTEM START MERGES t_packed_s3_no_spill;

OPTIMIZE TABLE t_packed_s3_spill FINAL;
OPTIMIZE TABLE t_packed_s3_no_spill FINAL;

SELECT (SELECT sum(cityHash64(*)) FROM t_packed_s3_spill) = (SELECT sum(cityHash64(*)) FROM t_packed_s3_no_spill);
SELECT DISTINCT part_storage_type FROM system.parts
WHERE database = currentDatabase() AND table IN ('t_packed_s3_spill', 't_packed_s3_no_spill') AND active;

SYSTEM FLUSH LOGS query_log, part_log, blob_storage_log;

-- Every part created by an insert is a single object (one PutObject or one multipart upload),
-- and the insert reads nothing from object storage.
SELECT
    q.log_comment,
    q.written_objects = p.new_parts AS one_object_per_part,
    q.get_requests,
    q.spilled
FROM
(
    SELECT
        query_id,
        log_comment,
        ProfileEvents['S3PutObject'] + ProfileEvents['S3CreateMultipartUpload'] AS written_objects,
        ProfileEvents['S3GetObject'] AS get_requests,
        ProfileEvents['ExternalPackedPartWriterWritePart'] > 0 AS spilled
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05367_insert_spill', '05367_insert_no_spill')
) AS q
INNER JOIN
(
    SELECT query_id, count() AS new_parts
    FROM system.part_log
    WHERE database = currentDatabase() AND event_type = 'NewPart'
    GROUP BY query_id
) AS p USING (query_id)
ORDER BY q.log_comment;

-- The archive of a part is uploaded when the part is committed, after the profile events of `part_log` are collected,
-- so the uploads are counted in `blob_storage_log`. Every part created by an insert or a merge (in the background
-- or by OPTIMIZE) is a single uploaded object, its `data.packed`, and no other objects are uploaded.
SELECT
    t.name AS table,
    u.archive_uploads = p.created_parts AS one_object_per_part,
    u.other_uploads,
    p.spilled
FROM
(
    SELECT name, toString(uuid) AS uuid
    FROM system.tables
    WHERE database = currentDatabase() AND name IN ('t_packed_s3_spill', 't_packed_s3_no_spill')
) AS t
INNER JOIN
(
    SELECT
        extract(local_path, '^store/[^/]+/([^/]+)/') AS uuid,
        countIf(endsWith(local_path, '/data.packed')) AS archive_uploads,
        countIf(NOT endsWith(local_path, '/data.packed')) AS other_uploads
    FROM system.blob_storage_log
    WHERE event_date >= yesterday() AND event_type IN ('Upload', 'MultiPartUploadCreate')
        -- Files of the parts, not of the table itself such as `format_version.txt`.
        AND match(local_path, '^store/[^/]+/[^/]+/[^/]+/')
    GROUP BY uuid
) AS u USING (uuid)
INNER JOIN
(
    SELECT
        table AS name,
        countIf(event_type IN ('NewPart', 'MergeParts')) AS created_parts,
        max(ProfileEvents['ExternalPackedPartWriterWritePart'] > 0) AS spilled
    FROM system.part_log
    WHERE database = currentDatabase() AND table IN ('t_packed_s3_spill', 't_packed_s3_no_spill') AND error = 0
    GROUP BY table
) AS p USING (name)
ORDER BY table;

DROP TABLE t_packed_s3_spill;
DROP TABLE t_packed_s3_no_spill;
