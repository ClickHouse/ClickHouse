DROP TABLE IF EXISTS test_local_1;

CREATE TABLE test_local_1 (date Date, value UInt32) ENGINE = MergeTree(date, date, 8192);

SELECT char(NULL, DAYOFWEEK(toDateTime('10485.75', char(0., NULL)))) FROM merge(currentDatabase(), 'test_local_1') PREWHERE _table = 'test_loc\0l_1';

DROP TABLE IF EXISTS test_local_1;
