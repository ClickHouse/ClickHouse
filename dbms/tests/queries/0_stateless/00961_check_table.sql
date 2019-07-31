SET check_query_single_value_result = 0;
DROP TABLE IF EXISTS mt_table;

CREATE TABLE mt_table (d Date, key UInt64, data String) ENGINE = MergeTree() PARTITION BY toYYYYMM(d) ORDER BY key;

CHECK TABLE mt_table;

INSERT INTO mt_table VALUES (toDate('2019-01-02'), 1, 'Hello'), (toDate('2019-01-02'), 2, 'World');

CHECK TABLE mt_table;

INSERT INTO mt_table VALUES (toDate('2019-01-02'), 3, 'quick'), (toDate('2019-01-02'), 4, 'brown');

SELECT '========';

CHECK TABLE mt_table;

OPTIMIZE TABLE mt_table FINAL;

SELECT '========';

CHECK TABLE mt_table;

SELECT '========';

INSERT INTO mt_table VALUES (toDate('2019-02-03'), 5, '!'), (toDate('2019-02-03'), 6, '?');

CHECK TABLE mt_table;

SELECT '========';

INSERT INTO mt_table VALUES (toDate('2019-02-03'), 7, 'jump'), (toDate('2019-02-03'), 8, 'around');

OPTIMIZE TABLE mt_table FINAL;

CHECK TABLE mt_table PARTITION 201902;

DROP TABLE IF EXISTS mt_table;
