SET enable_analyzer = 1;

DESCRIBE (SELECT 1 + 1);
SELECT 1 + 1;

SELECT '--';

DESCRIBE (SELECT dummy + dummy);
SELECT dummy + dummy;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value');

SELECT '--';

DESCRIBE (SELECT id + length(value) FROM test_table);
SELECT id + length(value) FROM test_table;

SELECT '--';

DESCRIBE (SELECT concat(concat(toString(id), '_'), (value)) FROM test_table);
SELECT concat(concat(toString(id), '_'), (value)) FROM test_table;
