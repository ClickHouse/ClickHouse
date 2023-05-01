SELECT arrayExists(x -> ((x.1) = 'pattern'), cast([tuple('a', 1)] as Array(Tuple(LowCardinality(String), UInt8))));

DROP TABLE IF EXISTS table;
CREATE TABLE table (id Int32, values Array(Tuple(LowCardinality(String), Int32)), date Date) ENGINE MergeTree() PARTITION BY toYYYYMM(date) ORDER BY (id, date);

SELECT count(*) FROM table WHERE (arrayExists(x -> ((x.1) = toLowCardinality('pattern')), values) = 1);

DROP TABLE IF EXISTS table;
