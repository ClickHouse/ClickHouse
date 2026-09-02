SET check_query_single_value_result = 0;
DROP TABLE IF EXISTS mt_table;

CREATE TABLE mt_table (d Date, key UInt64, data String) ENGINE = MergeTree() PARTITION BY toYYYYMM(d) ORDER BY key;

CHECK TABLE mt_table SETTINGS max_threads = 1;

INSERT INTO mt_table VALUES (toDate('2018-01-01'), 1, 'old');

INSERT INTO mt_table VALUES (toDate('2019-01-02'), 1, 'Hello'), (toDate('2019-01-02'), 2, 'World');

CHECK TABLE mt_table SETTINGS max_threads = 1;

INSERT INTO mt_table VALUES (toDate('2019-01-02'), 3, 'quick'), (toDate('2019-01-02'), 4, 'brown');

SELECT '========';

CHECK TABLE mt_table SETTINGS max_threads = 1;

OPTIMIZE TABLE mt_table FINAL;

SELECT '========';

CHECK TABLE mt_table SETTINGS max_threads = 1;

SELECT '========';

INSERT INTO mt_table VALUES (toDate('2019-02-03'), 5, '!'), (toDate('2019-02-03'), 6, '?');

CHECK TABLE mt_table SETTINGS max_threads = 1;

SELECT '========';

INSERT INTO mt_table VALUES (toDate('2019-02-03'), 7, 'jump'), (toDate('2019-02-03'), 8, 'around');

OPTIMIZE TABLE mt_table FINAL;

CHECK TABLE mt_table PARTITION 201902 SETTINGS max_threads = 1;

SELECT '========';

CHECK TABLE mt_table PART '201801_1_1_2';

DROP TABLE IF EXISTS mt_table;
