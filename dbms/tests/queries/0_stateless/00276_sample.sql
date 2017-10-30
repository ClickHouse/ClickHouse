DROP TABLE IF EXISTS test.sample;

SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
SET max_block_size = 10;

CREATE TABLE test.sample (d Date DEFAULT '2000-01-01', x UInt8) ENGINE = MergeTree(d, x, x, 10);
INSERT INTO test.sample (x) SELECT toUInt8(number) AS x FROM system.numbers LIMIT 256;

SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 1;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 0.1;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 1/10;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 1/1e1;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 1e1/1e2;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 1e-1;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 2e-2;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 1/10 OFFSET 1/10;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 1/10 OFFSET 9/10;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 1/10 OFFSET 10/10;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 1/10 OFFSET 19/20;

SELECT count() >= 100 FROM test.sample SAMPLE 100;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 1000;

SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 1/2;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 1/2 OFFSET 1/2;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 1/2 SETTINGS parallel_replicas_count = 3;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 1/2 SETTINGS parallel_replicas_count = 3, parallel_replica_offset = 0;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 1/2 SETTINGS parallel_replicas_count = 3, parallel_replica_offset = 1;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 1/2 SETTINGS parallel_replicas_count = 3, parallel_replica_offset = 2;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 1/2 OFFSET 1/2 SETTINGS parallel_replicas_count = 3;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 1/2 OFFSET 1/2 SETTINGS parallel_replicas_count = 3, parallel_replica_offset = 0;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 1/2 OFFSET 1/2 SETTINGS parallel_replicas_count = 3, parallel_replica_offset = 1;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 1/2 OFFSET 1/2 SETTINGS parallel_replicas_count = 3, parallel_replica_offset = 2;


SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM
(
          SELECT x FROM test.sample SAMPLE 0.1 OFFSET 0.0
UNION ALL SELECT x FROM test.sample SAMPLE 0.1 OFFSET 0.1
UNION ALL SELECT x FROM test.sample SAMPLE 0.1 OFFSET 0.2
UNION ALL SELECT x FROM test.sample SAMPLE 0.1 OFFSET 0.3
UNION ALL SELECT x FROM test.sample SAMPLE 0.1 OFFSET 0.4
UNION ALL SELECT x FROM test.sample SAMPLE 0.1 OFFSET 0.5
UNION ALL SELECT x FROM test.sample SAMPLE 0.1 OFFSET 0.6
UNION ALL SELECT x FROM test.sample SAMPLE 0.1 OFFSET 0.7
UNION ALL SELECT x FROM test.sample SAMPLE 0.1 OFFSET 0.8
UNION ALL SELECT x FROM test.sample SAMPLE 0.1 OFFSET 0.9
);

SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 0.05 OFFSET 0.35;
SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.sample SAMPLE 0.05 OFFSET 0.4;

SELECT count()
FROM
(
    SELECT
        x,
        count() AS c
    FROM
    (
                  SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.00
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.01
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.02
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.03
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.04
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.05
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.06
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.07
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.08
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.09
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.10
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.11
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.12
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.13
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.14
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.15
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.16
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.17
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.18
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.19
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.20
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.21
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.22
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.23
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.24
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.25
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.26
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.27
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.28
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.29
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.30
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.31
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.32
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.33
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.34
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.35
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.36
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.37
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.38
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.39
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.40
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.41
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.42
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.43
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.44
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.45
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.46
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.47
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.48
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.49
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.50
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.51
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.52
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.53
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.54
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.55
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.56
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.57
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.58
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.59
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.60
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.61
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.62
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.63
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.64
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.65
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.66
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.67
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.68
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.69
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.70
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.71
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.72
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.73
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.74
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.75
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.76
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.77
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.78
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.79
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.80
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.81
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.82
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.83
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.84
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.85
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.86
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.87
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.88
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.89
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.90
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.91
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.92
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.93
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.94
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.95
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.96
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.97
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.98
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.99
    )
    GROUP BY x
    HAVING c = 1
    ORDER BY x ASC
);

DROP TABLE test.sample;

SET max_block_size = 8192;

CREATE TABLE test.sample (d Date DEFAULT '2000-01-01', x UInt16) ENGINE = MergeTree(d, x, x, 10);
INSERT INTO test.sample (x) SELECT toUInt16(number) AS x FROM system.numbers LIMIT 65536;

SELECT count()
FROM
(
    SELECT
        x,
        count() AS c
    FROM
    (
                  SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.00
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.01
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.02
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.03
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.04
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.05
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.06
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.07
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.08
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.09
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.10
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.11
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.12
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.13
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.14
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.15
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.16
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.17
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.18
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.19
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.20
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.21
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.22
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.23
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.24
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.25
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.26
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.27
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.28
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.29
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.30
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.31
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.32
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.33
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.34
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.35
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.36
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.37
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.38
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.39
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.40
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.41
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.42
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.43
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.44
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.45
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.46
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.47
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.48
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.49
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.50
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.51
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.52
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.53
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.54
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.55
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.56
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.57
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.58
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.59
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.60
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.61
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.62
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.63
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.64
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.65
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.66
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.67
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.68
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.69
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.70
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.71
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.72
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.73
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.74
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.75
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.76
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.77
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.78
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.79
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.80
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.81
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.82
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.83
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.84
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.85
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.86
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.87
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.88
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.89
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.90
        UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.91
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.92
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.93
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.94
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.95
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.96
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.97
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.98
    UNION ALL SELECT * FROM test.sample SAMPLE 0.01 OFFSET 0.99
    )
    GROUP BY x
    HAVING c = 1
    ORDER BY x ASC
);

DROP TABLE test.sample;
