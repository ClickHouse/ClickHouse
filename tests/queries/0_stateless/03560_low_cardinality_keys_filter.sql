DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    s LowCardinality(String),
    client_name String,
) ENGINE = MergeTree ORDER BY ();

INSERT INTO test
SELECT number < 8000 ? 'ok' : 'fail' AS s,
       number < 8000 ? 'client1' : 'client2' FROM numbers(20000);

SELECT DISTINCT lowCardinalityKeys(s) FROM test PREWHERE client_name = 'client1' ORDER BY ALL;

DROP TABLE test;
