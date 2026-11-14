-- Tags: shard

-- What an INSERT requires of its destination is never sent with its source reads:
-- the settings are mandatory on the wire, so an older source server would refuse the read.

DROP TABLE IF EXISTS insert_expected_source_dst;
DROP TABLE IF EXISTS insert_expected_source_dst_dist;

CREATE TABLE insert_expected_source_dst (x UInt8) ENGINE = Memory;
CREATE TABLE insert_expected_source_dst_dist AS insert_expected_source_dst ENGINE = Distributed(test_shard_localhost, currentDatabase(), insert_expected_source_dst);

-- A local destination, the requirement written after the SELECT.
INSERT INTO insert_expected_source_dst SELECT dummy FROM cluster('test_shard_localhost', system.one)
SETTINGS insert_expected_table_engine = 'Memory', prefer_localhost_replica = 0, parallel_distributed_insert_select = 0, log_comment = '05099_source_reads_local';

-- A Distributed destination keeps the requirement for its shard's insert, but not for the source read.
INSERT INTO insert_expected_source_dst_dist SETTINGS insert_expected_table_engine = 'Memory', distributed_foreground_insert = 1, prefer_localhost_replica = 0, parallel_distributed_insert_select = 0, log_comment = '05099_source_reads_dist'
SELECT dummy + 1 FROM cluster('test_shard_localhost', system.one);

-- A *Cluster source sends its query text to the workers, which must not carry the requirement either.
INSERT INTO insert_expected_source_dst SELECT x + 2 FROM urlCluster('test_shard_localhost', 'http://localhost:8123/?query=SELECT+0', 'CSV', 'x UInt8')
SETTINGS insert_expected_table_engine = 'Memory', insert_expected_column_types = {'x': 'UInt8'}, parallel_distributed_insert_select = 0, log_comment = '05099_source_reads_url';

SYSTEM FLUSH LOGS query_log;

SELECT DISTINCT l.log_comment, splitByChar(' ', s.query)[1] AS kind, s.Settings['insert_expected_table_engine'] AS expected
FROM system.query_log AS s
INNER JOIN
(
    SELECT query_id, log_comment FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
        AND log_comment IN ('05099_source_reads_local', '05099_source_reads_dist')
) AS l ON s.initial_query_id = l.query_id
WHERE s.is_initial_query = 0 AND s.type = 'QueryFinish' AND s.event_date >= yesterday()
ORDER BY ALL;

SELECT l.log_comment, splitByChar(' ', s.query)[1] AS kind, s.Settings['insert_expected_table_engine'] AS expected, position(s.query, 'insert_expected') AS in_text
FROM system.query_log AS s
INNER JOIN
(
    SELECT query_id, log_comment FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
        AND log_comment = '05099_source_reads_url'
) AS l ON s.initial_query_id = l.query_id
WHERE s.is_initial_query = 0 AND s.type = 'QueryFinish' AND s.event_date >= yesterday();

SELECT * FROM insert_expected_source_dst ORDER BY x;

DROP TABLE insert_expected_source_dst_dist;
DROP TABLE insert_expected_source_dst;
