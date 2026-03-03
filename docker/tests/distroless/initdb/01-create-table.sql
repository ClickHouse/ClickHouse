CREATE TABLE IF NOT EXISTS test_db.events
(
    id       UInt64,
    name     String,
    ts       DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;
