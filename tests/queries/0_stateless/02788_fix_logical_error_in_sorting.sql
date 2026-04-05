SET allow_deprecated_error_prone_window_functions = 1;

DROP TABLE IF EXISTS session_events;
DROP TABLE IF EXISTS event_types;

CREATE TABLE session_events
(
    clientId UInt64,
    sessionId String,
    pageId UInt64,
    eventNumber UInt64,
    timestamp UInt64,
    type LowCardinality(String),
    data String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDate(pageId / 1000))
ORDER BY (clientId, sessionId, pageId, timestamp);

CREATE TABLE event_types
(
    type String,
    active Int16
)
ENGINE = MergeTree
PARTITION BY substring(type, 1, 1)
ORDER BY (type, active);

SYSTEM STOP MERGES session_events;
SYSTEM STOP MERGES event_types;

INSERT INTO session_events SELECT
    141,
    '693de636-6d9b-47b7-b52a-33bd303b6255',
    1686053240314,
    number,
    number,
    toString(number % 10),
    ''
FROM numbers_mt(100000);

INSERT INTO session_events SELECT
    141,
    '693de636-6d9b-47b7-b52a-33bd303b6255',
    1686053240314,
    number,
    number,
    toString(number % 10),
    ''
FROM numbers_mt(100000);

INSERT INTO event_types SELECT
    toString(number % 10),
    number % 2
FROM numbers(20);

SET optimize_sorting_by_input_stream_properties = 1;

-- We check only that no exception was thrown
EXPLAIN PIPELINE
SELECT
    pageId,
    [prev_active_ts, timestamp] AS inactivity_timestamps,
    timestamp - prev_active_ts AS inactive_duration,
    timestamp
FROM
(
    SELECT
        pageId,
        timestamp,
        neighbor(timestamp, -1) AS prev_active_ts
    FROM session_events
    WHERE (type IN (
        SELECT type
        FROM event_types
        WHERE active = 1
    )) AND (sessionId = '693de636-6d9b-47b7-b52a-33bd303b6255') AND (session_events.clientId = 141) AND (pageId = 1686053240314)
    ORDER BY timestamp ASC
)
WHERE runningDifference(timestamp) >= 500
ORDER BY timestamp ASC
FORMAT Null;

DROP TABLE session_events;
DROP TABLE event_types;
