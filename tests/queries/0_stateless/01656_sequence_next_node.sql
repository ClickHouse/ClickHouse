DROP TABLE IF EXISTS test_sequenceNextNode_Nullable;

CREATE TABLE iF NOT EXISTS test_sequenceNextNode_Nullable (dt DateTime, id int, action Nullable(String)) ENGINE = MergeTree() PARTITION BY dt ORDER BY id;

INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:01',1,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:02',1,'B');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:03',1,'C');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:04',1,'D');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:01',2,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:02',2,'B');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:03',2,'D');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:04',2,'C');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:01',3,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:02',3,'B');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:01',4,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:02',4,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:03',4,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:04',4,'B');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:05',4,'C');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:01',5,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:02',5,'B');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:03',5,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:04',5,'C');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:01',6,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:02',6,'B');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:03',6,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:04',6,'B');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:05',6,'C');

SELECT '(0, A)', id, sequenceNextNode(0)(dt, action, action = 'A') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(0, B)', id, sequenceNextNode(0)(dt, action, action = 'B') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(0, C)', id, sequenceNextNode(0)(dt, action, action = 'C') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(0, D)', id, sequenceNextNode(0)(dt, action, action = 'D') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(0, E)', id, sequenceNextNode(0)(dt, action, action = 'E') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(1, A)', id, sequenceNextNode(1)(dt, action, action = 'A') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(1, B)', id, sequenceNextNode(1)(dt, action, action = 'B') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(1, C)', id, sequenceNextNode(1)(dt, action, action = 'C') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(1, D)', id, sequenceNextNode(1)(dt, action, action = 'D') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(1, E)', id, sequenceNextNode(1)(dt, action, action = 'E') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(0, A->B)', id, sequenceNextNode(0)(dt, action, action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(0, A->C)', id, sequenceNextNode(0)(dt, action, action = 'A', action = 'C') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(0, B->A)', id, sequenceNextNode(0)(dt, action, action = 'B', action = 'A') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(1, A->B)', id, sequenceNextNode(1)(dt, action, action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(1, A->C)', id, sequenceNextNode(1)(dt, action, action = 'A', action = 'C') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(1, B->A)', id, sequenceNextNode(1)(dt, action, action = 'B', action = 'A') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(0, A->A->B)', id, sequenceNextNode(0)(dt, action, action = 'A', action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(0, B->A->A)', id, sequenceNextNode(0)(dt, action, action = 'B', action = 'A', action = 'A') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(1, A->A->B)', id, sequenceNextNode(1)(dt, action, action = 'A', action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(1, B->A->A)', id, sequenceNextNode(1)(dt, action, action = 'B', action = 'A', action = 'A') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;

INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:01',10,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:02',10,'B');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:02',10,NULL);
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:03',10,'C');
INSERT INTO test_sequenceNextNode_Nullable values ('1970-01-01 09:00:04',10,'D');

SELECT '(0, A) id >= 10', id, sequenceNextNode(0)(dt, action, action = 'A') AS next_node FROM test_sequenceNextNode_Nullable WHERE id >= 10 GROUP BY id ORDER BY id;
SELECT '(0, A) id >= 10', id, sequenceNextNode(0)(dt, action, action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode_Nullable WHERE id >= 10 GROUP BY id ORDER BY id;
SELECT '(0, A) id >= 10', id, sequenceNextNode(0)(dt, action, action = 'B', action = 'C') AS next_node FROM test_sequenceNextNode_Nullable WHERE id >= 10 GROUP BY id ORDER BY id;
SELECT '(0, A) id >= 10', id, sequenceNextNode(1)(dt, action, action = 'C') AS next_node FROM test_sequenceNextNode_Nullable WHERE id >= 10 GROUP BY id ORDER BY id;
SELECT '(0, A) id >= 10', id, sequenceNextNode(1)(dt, action, action = 'D', action = 'C') AS next_node FROM test_sequenceNextNode_Nullable WHERE id >= 10 GROUP BY id ORDER BY id;
SELECT '(0, A) id >= 10', id, sequenceNextNode(1)(dt, action, action = 'C', action = 'B') AS next_node FROM test_sequenceNextNode_Nullable WHERE id >= 10 GROUP BY id ORDER BY id;

DROP TABLE IF EXISTS test_sequenceNextNode_Nullable;

-- The same testcases for a non-null type.

DROP TABLE IF EXISTS test_sequenceNextNode;

CREATE TABLE iF NOT EXISTS test_sequenceNextNode (dt DateTime, id int, action String) ENGINE = MergeTree() PARTITION BY dt ORDER BY id;

INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:01',1,'A');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:02',1,'B');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:03',1,'C');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:04',1,'D');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:01',2,'A');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:02',2,'B');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:03',2,'D');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:04',2,'C');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:01',3,'A');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:02',3,'B');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:01',4,'A');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:02',4,'A');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:03',4,'A');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:04',4,'B');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:05',4,'C');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:01',5,'A');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:02',5,'B');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:03',5,'A');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:04',5,'C');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:01',6,'A');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:02',6,'B');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:03',6,'A');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:04',6,'B');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:05',6,'C');

SELECT '(0, A)', id, sequenceNextNode(0)(dt, action, action = 'A') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(0, B)', id, sequenceNextNode(0)(dt, action, action = 'B') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(0, C)', id, sequenceNextNode(0)(dt, action, action = 'C') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(0, D)', id, sequenceNextNode(0)(dt, action, action = 'D') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(0, E)', id, sequenceNextNode(0)(dt, action, action = 'E') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(1, A)', id, sequenceNextNode(1)(dt, action, action = 'A') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(1, B)', id, sequenceNextNode(1)(dt, action, action = 'B') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(1, C)', id, sequenceNextNode(1)(dt, action, action = 'C') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(1, D)', id, sequenceNextNode(1)(dt, action, action = 'D') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(1, E)', id, sequenceNextNode(1)(dt, action, action = 'E') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(0, A->B)', id, sequenceNextNode(0)(dt, action, action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(0, A->C)', id, sequenceNextNode(0)(dt, action, action = 'A', action = 'C') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(0, B->A)', id, sequenceNextNode(0)(dt, action, action = 'B', action = 'A') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(1, A->B)', id, sequenceNextNode(1)(dt, action, action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(1, A->C)', id, sequenceNextNode(1)(dt, action, action = 'A', action = 'C') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(1, B->A)', id, sequenceNextNode(1)(dt, action, action = 'B', action = 'A') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(0, A->A->B)', id, sequenceNextNode(0)(dt, action, action = 'A', action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(0, B->A->A)', id, sequenceNextNode(0)(dt, action, action = 'B', action = 'A', action = 'A') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(1, A->A->B)', id, sequenceNextNode(1)(dt, action, action = 'A', action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(1, B->A->A)', id, sequenceNextNode(1)(dt, action, action = 'B', action = 'A', action = 'A') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;

INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:01',10,'A');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:02',10,'B');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:03',10,'C');
INSERT INTO test_sequenceNextNode values ('1970-01-01 09:00:04',10,'D');

SELECT '(0, A) id >= 10', id, sequenceNextNode(0)(dt, action, action = 'A') AS next_node FROM test_sequenceNextNode WHERE id >= 10 GROUP BY id ORDER BY id;
SELECT '(0, A) id >= 10', id, sequenceNextNode(0)(dt, action, action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode WHERE id >= 10 GROUP BY id ORDER BY id;
SELECT '(0, A) id >= 10', id, sequenceNextNode(0)(dt, action, action = 'B', action = 'C') AS next_node FROM test_sequenceNextNode WHERE id >= 10 GROUP BY id ORDER BY id;
SELECT '(0, A) id >= 10', id, sequenceNextNode(1)(dt, action, action = 'C') AS next_node FROM test_sequenceNextNode WHERE id >= 10 GROUP BY id ORDER BY id;
SELECT '(0, A) id >= 10', id, sequenceNextNode(1)(dt, action, action = 'D', action = 'C') AS next_node FROM test_sequenceNextNode WHERE id >= 10 GROUP BY id ORDER BY id;
SELECT '(0, A) id >= 10', id, sequenceNextNode(1)(dt, action, action = 'C', action = 'B') AS next_node FROM test_sequenceNextNode WHERE id >= 10 GROUP BY id ORDER BY id;

DROP TABLE IF EXISTS test_sequenceNextNode;
