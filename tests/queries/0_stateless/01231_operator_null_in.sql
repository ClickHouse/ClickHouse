DROP TABLE IF EXISTS null_in;
CREATE TABLE null_in (dt DateTime, idx int, i Nullable(int), s Nullable(String)) ENGINE = MergeTree() PARTITION BY dt ORDER BY idx;

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


DROP TABLE IF EXISTS null_in_subquery;
CREATE TABLE null_in_subquery (dt DateTime, idx int, i Nullable(UInt64)) ENGINE = MergeTree() PARTITION BY dt ORDER BY idx;
INSERT INTO null_in_subquery SELECT number % 3, number, number FROM system.numbers LIMIT 99999;

SELECT count() == 33333 FROM null_in_subquery WHERE i in (SELECT i FROM null_in_subquery WHERE dt = 0);
SELECT count() == 66666 FROM null_in_subquery WHERE i not in (SELECT i FROM null_in_subquery WHERE dt = 1);
SELECT count() == 33333 FROM null_in_subquery WHERE i global in (SELECT i FROM null_in_subquery WHERE dt = 2);
SELECT count() == 66666 FROM null_in_subquery WHERE i global not in (SELECT i FROM null_in_subquery WHERE dt = 0);

-- For index column
SELECT count() == 33333 FROM null_in_subquery WHERE idx in (SELECT idx FROM null_in_subquery WHERE dt = 0);
SELECT count() == 66666 FROM null_in_subquery WHERE idx not in (SELECT idx FROM null_in_subquery WHERE dt = 1);
SELECT count() == 33333 FROM null_in_subquery WHERE idx global in (SELECT idx FROM null_in_subquery WHERE dt = 2);
SELECT count() == 66666 FROM null_in_subquery WHERE idx global not in (SELECT idx FROM null_in_subquery WHERE dt = 0);

INSERT INTO null_in_subquery VALUES (0, 123456780, NULL);
INSERT INTO null_in_subquery VALUES (1, 123456781, NULL);

SELECT count() == 33335 FROM null_in_subquery WHERE i in (SELECT i FROM null_in_subquery WHERE dt = 0);
SELECT count() == 66666 FROM null_in_subquery WHERE i not in (SELECT i FROM null_in_subquery WHERE dt = 1);
SELECT count() == 33333 FROM null_in_subquery WHERE i in (SELECT i FROM null_in_subquery WHERE dt = 2);
SELECT count() == 66668 FROM null_in_subquery WHERE i not in (SELECT i FROM null_in_subquery WHERE dt = 2);
SELECT count() == 33335 FROM null_in_subquery WHERE i global in (SELECT i FROM null_in_subquery WHERE dt = 0);
SELECT count() == 66666 FROM null_in_subquery WHERE i global not in (SELECT i FROM null_in_subquery WHERE dt = 1);
SELECT count() == 33333 FROM null_in_subquery WHERE i global in (SELECT i FROM null_in_subquery WHERE dt = 2);
SELECT count() == 66668 FROM null_in_subquery WHERE i global not in (SELECT i FROM null_in_subquery WHERE dt = 2);

DROP TABLE IF EXISTS null_in_subquery;


DROP TABLE IF EXISTS null_in_tuple;
CREATE TABLE null_in_tuple (dt DateTime, idx int, t Tuple(Nullable(UInt64), Nullable(String))) ENGINE = MergeTree() PARTITION BY dt ORDER BY idx;
INSERT INTO null_in_tuple VALUES (1, 1, (1, '1')) (2, 2, (2, NULL)) (3, 3, (NULL, '3')) (4, 4, (NULL, NULL))

SET transform_null_in = 0;

SELECT arraySort(x -> (x.1, x.2), groupArray(t)) == [(1, '1')] FROM null_in_tuple WHERE t in ((1, '1'), (NULL, NULL));
SELECT arraySort(x -> (x.1, x.2), groupArray(t)) == [(2, NULL), (NULL, '3'), (NULL, NULL)] FROM null_in_tuple WHERE t not in ((1, '1'), (NULL, NULL));
SELECT arraySort(x -> (x.1, x.2), groupArray(t)) == [(1, '1')] FROM null_in_tuple WHERE t global in ((1, '1'), (NULL, NULL));
SELECT arraySort(x -> (x.1, x.2), groupArray(t)) == [(2, NULL), (NULL, '3'), (NULL, NULL)] FROM null_in_tuple WHERE t global not in ((1, '1'), (NULL, NULL));

SET transform_null_in = 1;

SELECT arraySort(x -> (x.1, x.2), groupArray(t)) == [(1, '1'), (NULL, NULL)] FROM null_in_tuple WHERE t in ((1, '1'), (NULL, NULL));
SELECT arraySort(x -> (x.1, x.2), groupArray(t)) == [(2, NULL), (NULL, '3')] FROM null_in_tuple WHERE t not in ((1, '1'), (NULL, NULL));
SELECT arraySort(x -> (x.1, x.2), groupArray(t)) == [(1, '1'), (NULL, NULL)] FROM null_in_tuple WHERE t global in ((1, '1'), (NULL, NULL));
SELECT arraySort(x -> (x.1, x.2), groupArray(t)) == [(2, NULL), (NULL, '3')] FROM null_in_tuple WHERE t global not in ((1, '1'), (NULL, NULL));

SELECT arraySort(x -> (x.1, x.2), groupArray(t)) == [(1, '1')] FROM null_in_tuple WHERE t in ((1, '1'), (1, NULL));
SELECT arraySort(x -> (x.1, x.2), groupArray(t)) == [(1, '1')] FROM null_in_tuple WHERE t in ((1, '1'), (NULL, '1'));
SELECT arraySort(x -> (x.1, x.2), groupArray(t)) == [(1, '1'), (2, NULL)] FROM null_in_tuple WHERE t in ((1, '1'), (NULL, '1'), (2, NULL));
SELECT arraySort(x -> (x.1, x.2), groupArray(t)) == [(1, '1'), (NULL, '3')] FROM null_in_tuple WHERE t in ((1, '1'), (1, NULL), (NULL, '3'));
SELECT arraySort(x -> (x.1, x.2), groupArray(t)) == [(1, '1'), (2, NULL), (NULL, '3'), (NULL, NULL)] FROM null_in_tuple WHERE t in ((1, '1'), (1, NULL), (2, NULL), (NULL, '3'), (NULL, NULL));

SELECT arraySort(x -> (x.1, x.2), groupArray(t)) == [(2, NULL), (NULL, '3'), (NULL, NULL)] FROM null_in_tuple WHERE t not in ((1, '1'), (1, NULL));
SELECT arraySort(x -> (x.1, x.2), groupArray(t)) == [(2, NULL), (NULL, '3'), (NULL, NULL)] FROM null_in_tuple WHERE t not in ((1, '1'), (NULL, '1'));
SELECT arraySort(x -> (x.1, x.2), groupArray(t)) == [(NULL, '3'), (NULL, NULL)] FROM null_in_tuple WHERE t not in ((1, '1'), (NULL, '1'), (2, NULL));
SELECT arraySort(x -> (x.1, x.2), groupArray(t)) == [(2, NULL), (NULL, NULL)] FROM null_in_tuple WHERE t not in ((1, '1'), (1, NULL), (NULL, '3'));
SELECT arraySort(x -> (x.1, x.2), groupArray(t)) == [] FROM null_in_tuple WHERE t not in ((1, '1'), (1, NULL), (2, NULL), (NULL, '3'), (NULL, NULL));

DROP TABLE IF EXISTS null_in_tuple;
