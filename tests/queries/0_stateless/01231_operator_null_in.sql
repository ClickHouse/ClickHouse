DROP TABLE IF EXISTS null_in;
CREATE TABLE null_in (dt DateTime, idx int, i Nullable(int), s Nullable(String)) ENGINE = MergeTree() PARTITION BY dt ORDER BY idx SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO null_in VALUES (1, 1, 1, '1') (2, 2, NULL, NULL) (3, 3, 3, '3') (4, 4, NULL, NULL) (5, 5, 5, '5');

SELECT count() == 2 FROM null_in WHERE i in (1, 3, NULL);
SELECT count() == 2 FROM null_in WHERE i in range(4);
SELECT count() == 2 FROM null_in WHERE s in ('1', '3', NULL);
SELECT count() == 2 FROM null_in WHERE i global in (1, 3, NULL);
SELECT count() == 2 FROM null_in WHERE i global in range(4);
SELECT count() == 2 FROM null_in WHERE s global in ('1', '3', NULL);

SELECT count() == 1 FROM null_in WHERE i not in (1, 3, NULL);
SELECT count() == 1 FROM null_in WHERE i not in range(4);
SELECT count() == 1 FROM null_in WHERE s not in ('1', '3', NULL);
SELECT count() == 1 FROM null_in WHERE i global not in (1, 3, NULL);
SELECT count() == 1 FROM null_in WHERE i global not in range(4);
SELECT count() == 1 FROM null_in WHERE s global not in ('1', '3', NULL);

SET transform_null_in = 1;

SELECT count() == 4 FROM null_in WHERE i in (1, 3, NULL);
SELECT count() == 2 FROM null_in WHERE i in range(4);
SELECT count() == 4 FROM null_in WHERE s in ('1', '3', NULL);
SELECT count() == 4 FROM null_in WHERE i global in (1, 3, NULL);
SELECT count() == 2 FROM null_in WHERE i global in range(4);
SELECT count() == 4 FROM null_in WHERE s global in ('1', '3', NULL);

SELECT count() == 1 FROM null_in WHERE i not in (1, 3, NULL);
SELECT count() == 3 FROM null_in WHERE i not in range(4);
SELECT count() == 1 FROM null_in WHERE s not in ('1', '3', NULL);
SELECT count() == 1 FROM null_in WHERE i global not in (1, 3, NULL);
SELECT count() == 3 FROM null_in WHERE i global not in range(4);
SELECT count() == 1 FROM null_in WHERE s global not in ('1', '3', NULL);

SELECT count() == 3 FROM null_in WHERE i not in (1, 3);
SELECT count() == 3 FROM null_in WHERE i not in range(4);
SELECT count() == 3 FROM null_in WHERE s not in ('1', '3');
SELECT count() == 3 FROM null_in WHERE i global not in (1, 3);
SELECT count() == 3 FROM null_in WHERE i global not in range(4);
SELECT count() == 3 FROM null_in WHERE s global not in ('1', '3');

DROP TABLE IF EXISTS test_set;
CREATE TABLE test_set (i Nullable(int)) ENGINE = Set();
INSERT INTO test_set VALUES (1), (NULL);

SET transform_null_in = 0;

SELECT count() == 1 FROM null_in WHERE i in test_set;
SELECT count() == 2 FROM null_in WHERE i not in test_set;
SELECT count() == 1 FROM null_in WHERE i global in test_set;
SELECT count() == 2 FROM null_in WHERE i global not in test_set;

SET transform_null_in = 1;

SELECT count() == 3 FROM null_in WHERE i in test_set;
SELECT count() == 2 FROM null_in WHERE i not in test_set;
SELECT count() == 3 FROM null_in WHERE i global in test_set;
SELECT count() == 2 FROM null_in WHERE i global not in test_set;

-- Create with transform_null_in
CREATE TABLE test_set2 (i Nullable(int)) ENGINE = Set();
INSERT INTO test_set2 VALUES (1), (NULL);

SET transform_null_in = 0;

SELECT count() == 1 FROM null_in WHERE i in test_set2;
SELECT count() == 2 FROM null_in WHERE i not in test_set2;
SELECT count() == 1 FROM null_in WHERE i global in test_set2;
SELECT count() == 2 FROM null_in WHERE i global not in test_set2;

SET transform_null_in = 1;

SELECT count() == 3 FROM null_in WHERE i in test_set2;
SELECT count() == 2 FROM null_in WHERE i not in test_set2;
SELECT count() == 3 FROM null_in WHERE i global in test_set2;
SELECT count() == 2 FROM null_in WHERE i global not in test_set2;

DROP TABLE IF EXISTS test_set;
DROP TABLE IF EXISTS null_in;
DROP TABLE test_set2;
