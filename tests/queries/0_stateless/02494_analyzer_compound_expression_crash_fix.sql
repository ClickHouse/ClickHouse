SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (
    fingerprint UInt16,
    fields Array(Tuple(name Array(UInt32), value String))
) ENGINE = MergeTree
ORDER BY fingerprint;

INSERT INTO test_table VALUES (0, [[1]], ['1']);

SELECT fields.name FROM (SELECT fields.name FROM test_table);

SELECT fields.name, fields.value FROM (SELECT fields.name FROM test_table); -- { serverError 47 }

DROP TABLE IF EXISTS test_table;
