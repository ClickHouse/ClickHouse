-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.

-- { echoOn }

SET enable_analyzer = 1;

DROP TABLE IF EXISTS events;

CREATE TABLE events
(
    uuid UUID,
    person_id UUID,
    event String,
    timestamp DateTime64(6, 'UTC'),
    team_id Int64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (team_id, toDate(timestamp), event, cityHash64(person_id), cityHash64(uuid));

INSERT INTO events (uuid, person_id, event, timestamp, team_id)
VALUES (
    '017adbc0-98b5-0000-3f74-619a368fe65d',
    '017adbc0-98b5-0000-3f74-619a368fe65d',
    'page_view',
    now64(6, 'UTC'),
    1
);

SELECT count()
FROM events e
WHERE person_id = '017adbc0-98b5-0000-3f74-619a368fe65e';

EXPLAIN indexes = 1
SELECT count()
FROM events e
WHERE person_id = '017adbc0-98b5-0000-3f74-619a368fe65e';

SELECT count()
FROM events e
WHERE person_id < '017adbc0-98b5-0000-3f74-619a368fe65e';

EXPLAIN indexes = 1
SELECT count()
FROM events e
WHERE person_id < '017adbc0-98b5-0000-3f74-619a368fe65e';

SELECT count()
FROM events e
WHERE person_id != '017adbc0-98b5-0000-3f74-619a368fe65e';

EXPLAIN indexes = 1
SELECT count()
FROM events e
WHERE person_id != '017adbc0-98b5-0000-3f74-619a368fe65e';


DROP TABLE IF EXISTS test;
CREATE TABLE test (
    pod String
)
ENGINE = MergeTree()
ORDER BY (left(pod, length(pod) - length(substringIndex(pod, '-', -1)) - 1))
SETTINGS index_granularity = 1000;

INSERT INTO test
SELECT
    arrayElement(
            ['vector-abc-001', 'vector-abc-002', 'metrics-def-003', 'metrics-def-004', 'logs-ghi-005', 'logs-ghi-006', 'traces-jkl-007', 'traces-jkl-008', 'events-mno-009', 'events-mno-010'],
            (number % 10) + 1
    )
FROM numbers(100000);

EXPLAIN indexes=1
SELECT count()
FROM test
WHERE pod = 'vector-abc-001';

SELECT count()
FROM test
WHERE pod = 'vector-abc-001';

EXPLAIN indexes=1
SELECT count()
FROM test
WHERE not pod = 'vector-abc-001';

SELECT count()
FROM test
WHERE not pod = 'vector-abc-001';

DROP TABLE IF EXISTS test_expr;
CREATE TABLE test_expr (
    pod String
)
ENGINE = MergeTree()
ORDER BY (left(lower(pod), length(lower(pod)) - length(substringIndex(lower(pod), '-', -1)) - 1))
SETTINGS index_granularity = 1000;


INSERT INTO test_expr
SELECT
    arrayElement(
            ['vector-abC-001', 'vector-abC-002', 'metrics-deF-003', 'metrics-deF-004', 'logs-ghI-005', 'logs-ghI-006', 'traces-jkL-007', 'traces-jkL-008', 'events-mnO-009', 'events-mnO-010'],
            (number % 10) + 1
    )
FROM numbers(100000);

EXPLAIN indexes=1
SELECT count()
FROM test_expr
WHERE lower(pod) = 'vector-abc-001';

SELECT count()
FROM test_expr
WHERE lower(pod) = 'vector-abc-001';

EXPLAIN indexes=1
SELECT count()
FROM test_expr
WHERE not lower(pod) = 'vector-abc-001';

SELECT count()
FROM test_expr
WHERE not lower(pod) = 'vector-abc-001';

SET enable_analyzer = 0;

DROP TABLE IF EXISTS events;

CREATE TABLE events
(
    uuid UUID,
    person_id UUID,
    event String,
    timestamp DateTime64(6, 'UTC'),
    team_id Int64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (team_id, toDate(timestamp), event, cityHash64(person_id), cityHash64(uuid));

INSERT INTO events (uuid, person_id, event, timestamp, team_id)
VALUES (
    '017adbc0-98b5-0000-3f74-619a368fe65d',
    '017adbc0-98b5-0000-3f74-619a368fe65d',
    'page_view',
    now64(6, 'UTC'),
    1
);

SELECT count()
FROM events e
WHERE person_id = '017adbc0-98b5-0000-3f74-619a368fe65e';

EXPLAIN indexes = 1
SELECT count()
FROM events e
WHERE person_id = '017adbc0-98b5-0000-3f74-619a368fe65e';

SELECT count()
FROM events e
WHERE person_id < '017adbc0-98b5-0000-3f74-619a368fe65e';

EXPLAIN indexes = 1
SELECT count()
FROM events e
WHERE person_id < '017adbc0-98b5-0000-3f74-619a368fe65e';

SELECT count()
FROM events e
WHERE person_id != '017adbc0-98b5-0000-3f74-619a368fe65e';

EXPLAIN indexes = 1
SELECT count()
FROM events e
WHERE person_id != '017adbc0-98b5-0000-3f74-619a368fe65e';


DROP TABLE IF EXISTS test;
CREATE TABLE test (
    pod String
)
ENGINE = MergeTree()
ORDER BY (left(pod, length(pod) - length(substringIndex(pod, '-', -1)) - 1))
SETTINGS index_granularity = 1000;

INSERT INTO test
SELECT
    arrayElement(
            ['vector-abc-001', 'vector-abc-002', 'metrics-def-003', 'metrics-def-004', 'logs-ghi-005', 'logs-ghi-006', 'traces-jkl-007', 'traces-jkl-008', 'events-mno-009', 'events-mno-010'],
            (number % 10) + 1
    )
FROM numbers(100000);

EXPLAIN indexes=1
SELECT count()
FROM test
WHERE pod = 'vector-abc-001';

SELECT count()
FROM test
WHERE pod = 'vector-abc-001';

EXPLAIN indexes=1
SELECT count()
FROM test
WHERE not pod = 'vector-abc-001';

SELECT count()
FROM test
WHERE not pod = 'vector-abc-001';

DROP TABLE IF EXISTS test_expr;
CREATE TABLE test_expr (
    pod String
)
ENGINE = MergeTree()
ORDER BY (left(lower(pod), length(lower(pod)) - length(substringIndex(lower(pod), '-', -1)) - 1))
SETTINGS index_granularity = 1000;


INSERT INTO test_expr
SELECT
    arrayElement(
            ['vector-abC-001', 'vector-abC-002', 'metrics-deF-003', 'metrics-deF-004', 'logs-ghI-005', 'logs-ghI-006', 'traces-jkL-007', 'traces-jkL-008', 'events-mnO-009', 'events-mnO-010'],
            (number % 10) + 1
    )
FROM numbers(100000);

EXPLAIN indexes=1
SELECT count()
FROM test_expr
WHERE lower(pod) = 'vector-abc-001';

SELECT count()
FROM test_expr
WHERE lower(pod) = 'vector-abc-001';

EXPLAIN indexes=1
SELECT count()
FROM test_expr
WHERE not lower(pod) = 'vector-abc-001';

SELECT count()
FROM test_expr
WHERE not lower(pod) = 'vector-abc-001';
