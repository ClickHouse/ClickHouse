-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

-- system.blob_storage_log records which HTTP connection carried each object storage request and how
-- worn that connection was, so a request that landed on a freshly opened socket can be told apart
-- from one that reused a warm keep-alive connection. The two differ several-fold in time to first
-- byte, and nothing else the server exposes distinguishes them: DiskConnectionsReused says a session
-- came out of the pool, not how long it had been sitting there.

SET enable_blob_storage_log_for_read_operations = 1;

INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/04106_data/file_'||currentDatabase()||'.csv', structure = 'number UInt64', format = CSV)
    SETTINGS s3_truncate_on_insert = 1
    SELECT number FROM numbers(100);

SELECT sum(number) FROM s3(s3_conn, url = 'http://localhost:11111/test/04106_data/file_'||currentDatabase()||'.csv', structure = 'number UInt64', format = CSV);
SELECT sum(number) FROM s3(s3_conn, url = 'http://localhost:11111/test/04106_data/file_'||currentDatabase()||'.csv', structure = 'number UInt64', format = CSV);

SYSTEM FLUSH LOGS blob_storage_log;

-- Every request that went over HTTP is attributed to a connection, with a port and an inode, and the
-- connection is at most as old as this test run.
SELECT
    count() > 0,
    countIf(connection_id = 0) = 0,
    countIf(connection_local_port = 0) = 0,
    countIf(connection_socket_inode = 0) = 0,
    countIf(connection_age_microseconds > 3600000000) = 0
FROM system.blob_storage_log
WHERE remote_path LIKE '%04106_data/file_' || currentDatabase() || '.csv'
    AND event_date >= yesterday()
    AND event_time > now() - INTERVAL 5 MINUTE;

-- A connection id identifies one socket, so the port and inode seen under it never change. (The
-- reverse does not hold: the OS recycles both once a socket closes, which is why the id exists.)
SELECT max(ports) = 1, max(inodes) = 1
FROM (
    SELECT uniqExact(connection_local_port) AS ports, uniqExact(connection_socket_inode) AS inodes
    FROM system.blob_storage_log
    WHERE remote_path LIKE '%04106_data/file_' || currentDatabase() || '.csv'
        AND event_date >= yesterday()
        AND event_time > now() - INTERVAL 5 MINUTE
    GROUP BY connection_id
);

-- A socket's first request has nothing to be idle after, and no request can have been idle for
-- longer than its socket has existed.
SELECT
    countIf(connection_requests = 0 AND connection_idle_microseconds != 0) = 0,
    countIf(connection_idle_microseconds > connection_age_microseconds) = 0
FROM system.blob_storage_log
WHERE remote_path LIKE '%04106_data/file_' || currentDatabase() || '.csv'
    AND event_date >= yesterday()
    AND event_time > now() - INTERVAL 5 MINUTE;

-- The point of the feature: a keep-alive socket keeps its identity across the requests it serves.
-- These three queries run one after another against the same endpoint, so the socket the first one
-- opened is handed back out to the next, and at least one request must find a connection that has
-- already sent something. Without this assertion every check above still passes when the pool mints
-- a fresh connection_id on every borrow - which makes the columns useless for following a socket.
SELECT max(connection_requests) > 0
FROM system.blob_storage_log
WHERE remote_path LIKE '%04106_data/file_' || currentDatabase() || '.csv'
    AND event_date >= yesterday()
    AND event_time > now() - INTERVAL 5 MINUTE;
