-- Tags: no-fasttest
DROP TABLE IF EXISTS t;
CREATE TABLE t
(
    `n1` Int32
)
ENGINE = File(Avro)
SETTINGS output_format_avro_codec = 'zstd';

INSERT INTO t SELECT *
FROM numbers(10);

SELECT sum(n1) 
FROM t;

DROP TABLE t;
