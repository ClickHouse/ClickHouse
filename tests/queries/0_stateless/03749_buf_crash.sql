DROP TABLE IF EXISTS left_bio;


DROP TABLE IF EXISTS tickets_src;

CREATE TABLE left_bio
(
    workerScope_hash String,
    rockman_id String
)
ENGINE = MergeTree
ORDER BY workerScope_hash;

CREATE TABLE tickets_src
(
    dummy UInt8,
    rockman_id Nullable(String)
)
ENGINE = MergeTree
ORDER BY dummy;

INSERT INTO left_bio VALUES
    ('hash1', 'rockman-123-456'),
    ('hash2', 'rockman-789-012');

INSERT INTO tickets_src VALUES
    (0, 'rockman-123-456'),
    (1, NULL);


WITH
filtered_biometrics AS
(
    SELECT
        workerScope_hash,
        rockman_id
    FROM left_bio
),
tickets AS
(
    -- rockman_id here is Nullable(String)
    SELECT rockman_id
    FROM tickets_src
)
SELECT
    workerScope_hash,
    count(DISTINCT rockman_id)         AS sessions,
    count(DISTINCT tickets.rockman_id) AS tickets
FROM filtered_biometrics
LEFT JOIN tickets USING (rockman_id)
GROUP BY workerScope_hash
ORDER BY workerScope_hash;
