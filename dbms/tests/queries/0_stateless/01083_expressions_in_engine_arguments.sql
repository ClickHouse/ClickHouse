DROP DATABASE IF EXISTS test_01083;
CREATE DATABASE test_01083;
USE test_01083;

CREATE TABLE file (n Int8) ENGINE = File(upper('tsv') || 'WithNames' || 'AndTypes');
CREATE TABLE buffer (n Int8) ENGINE = Buffer(currentDatabase(), file, 16, 10, 200, 10000, 1000000, 10000000, 1000000000);
CREATE TABLE merge (n Int8) ENGINE = Merge('', lower('FILE'));
CREATE TABLE merge_tf as merge(currentDatabase(), '.*');
CREATE TABLE distributed (n Int8) ENGINE = Distributed(test_cluster, currentDatabase(), 'fi' || 'le');
CREATE TABLE distributed_tf as cluster('test' || '_' || 'cluster', currentDatabase(), 'fi' || 'le');

SHOW CREATE file;
SHOW CREATE buffer;
SHOW CREATE merge;
SHOW CREATE merge_tf;
SHOW CREATE distributed;
SHOW CREATE distributed_tf;

DROP DATABASE test_01083;
