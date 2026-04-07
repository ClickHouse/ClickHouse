DROP TABLE IF EXISTS test_low_cardinality_string;
DROP TABLE IF EXISTS test_low_cardinality_uuid;
DROP TABLE IF EXISTS test_low_cardinality_int;
CREATE TABLE test_low_cardinality_string (data String) ENGINE MergeTree ORDER BY data;
CREATE TABLE test_low_cardinality_uuid (data String) ENGINE MergeTree ORDER BY data;
CREATE TABLE test_low_cardinality_int (data String) ENGINE MergeTree ORDER BY data;
INSERT INTO test_low_cardinality_string (data) VALUES ('{"a": "hi", "b": "hello", "c": "hola", "d": "see you, bye, bye"}');
INSERT INTO test_low_cardinality_int (data) VALUES ('{"a": 11, "b": 2222, "c": 33333333, "d": 4444444444444444}');
INSERT INTO test_low_cardinality_uuid (data) VALUES ('{"a": "2d49dc6e-ddce-4cd0-afb8-790956df54c4", "b": "2d49dc6e-ddce-4cd0-afb8-790956df54c3", "c": "2d49dc6e-ddce-4cd0-afb8-790956df54c1", "d": "2d49dc6e-ddce-4cd0-afb8-790956df54c1"}');
SELECT JSONExtract(data, 'Tuple(
                            a LowCardinality(String),
                            b LowCardinality(String),
                            c LowCardinality(String),
                            d LowCardinality(String)
                            )') AS json FROM test_low_cardinality_string;
SELECT JSONExtract(data, 'Tuple(
                            a LowCardinality(FixedString(20)),
                            b LowCardinality(FixedString(20)),
                            c LowCardinality(FixedString(20)),
                            d LowCardinality(FixedString(20))
                            )') AS json FROM test_low_cardinality_string;
SELECT JSONExtract(data, 'Tuple(
                            a LowCardinality(Int8),
                            b LowCardinality(Int8),
                            c LowCardinality(Int8),
                            d LowCardinality(Int8)
                            )') AS json FROM test_low_cardinality_int;
SELECT JSONExtract(data, 'Tuple(
                            a LowCardinality(Int16),
                            b LowCardinality(Int16),
                            c LowCardinality(Int16),
                            d LowCardinality(Int16)
                            )') AS json FROM test_low_cardinality_int;
SELECT JSONExtract(data, 'Tuple(
                            a LowCardinality(Int32),
                            b LowCardinality(Int32),
                            c LowCardinality(Int32),
                            d LowCardinality(Int32)
                            )') AS json FROM test_low_cardinality_int;
SELECT JSONExtract(data, 'Tuple(
                            a LowCardinality(Int64),
                            b LowCardinality(Int64),
                            c LowCardinality(Int64),
                            d LowCardinality(Int64)
                            )') AS json FROM test_low_cardinality_int;
SELECT JSONExtract(data, 'Tuple(
                            a LowCardinality(UUID),
                            b LowCardinality(UUID),
                            c LowCardinality(UUID),
                            d LowCardinality(UUID)
                            )') AS json FROM test_low_cardinality_uuid;
DROP TABLE test_low_cardinality_string;
DROP TABLE test_low_cardinality_uuid;
DROP TABLE test_low_cardinality_int;
