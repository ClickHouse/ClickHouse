DROP TABLE IF EXISTS testing;

CREATE TABLE testing
(
    a String,
    b String,
    c String,
    d String,
    PROJECTION proj_1
    (
        SELECT b, c
        ORDER BY d
    )
)
ENGINE = MergeTree()
PRIMARY KEY (a)
ORDER BY (a, b)
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO testing SELECT randomString(5), randomString(5), randomString(5), randomString(5) FROM numbers(10);

OPTIMIZE TABLE testing FINAL;

ALTER TABLE testing MODIFY COLUMN c LowCardinality(String) SETTINGS mutations_sync=2;

SELECT * FROM system.mutations WHERE database = currentDatabase() AND table = 'testing' AND not is_done;

DROP TABLE testing;
