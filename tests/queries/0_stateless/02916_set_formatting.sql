SELECT formatQuerySingleLine('set additional_table_filters = {\'kjsnckjn\': \'ksanmn\', \'dkm\': \'dd\'}');
SELECT formatQuerySingleLine('SELECT v FROM t1 SETTINGS additional_table_filters = {\'default.t1\': \'s\'}');

DROP TABLE IF EXISTS t1;
DROP VIEW IF EXISTS v1;

CREATE TABLE t1 (v UInt64, s String) ENGINE=MergeTree() ORDER BY v;
CREATE VIEW v1 (v UInt64) AS SELECT v FROM t1 SETTINGS additional_table_filters = {'default.t1': 's != \'s1%\''};

SHOW CREATE TABLE v1 FORMAT Vertical;

DROP VIEW v1;
DROP TABLE t1;
