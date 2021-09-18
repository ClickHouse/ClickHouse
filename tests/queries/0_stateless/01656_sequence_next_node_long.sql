-- Tags: long

SET allow_experimental_funnel_functions = 1;

DROP TABLE IF EXISTS test_sequenceNextNode_Nullable;

CREATE TABLE IF NOT EXISTS test_sequenceNextNode_Nullable (dt DateTime, id int, action Nullable(String)) ENGINE = MergeTree() PARTITION BY dt ORDER BY id;

INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:01',1,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:02',1,'B');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:03',1,'C');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:04',1,'D');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:01',2,'B');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:02',2,'B');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:03',2,'D');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:04',2,'C');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:01',3,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:02',3,'B');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:01',4,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:02',4,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:03',4,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:04',4,'B');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:05',4,'C');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:01',5,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:02',5,'B');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:03',5,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:04',5,'C');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:01',6,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:02',6,'B');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:03',6,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:04',6,'B');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:05',6,'C');

SELECT '(forward, head, A)', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'A') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(forward, head, B)', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'B') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(forward, head, C)', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'C') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(forward, head, D)', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'D') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(forward, head, E)', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'E') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(backward, tail, A)', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'A') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(backward, tail, B)', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'B') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(backward, tail, C)', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'C') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(backward, tail, D)', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'D') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(backward, tail, E)', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'E') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(forward, head, A->B)', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(forward, head, A->C)', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'A', action = 'C') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(forward, head, B->A)', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'B', action = 'A') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(backward, tail, A->B)', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(backward, tail, A->C)', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'A', action = 'C') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(backward, tail, B->A)', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'B', action = 'A') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(forward, head, A->A->B)', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'A', action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(forward, head, B->A->A)', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'B', action = 'A', action = 'A') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(backward, tail, A->A->B)', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'A', action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(backward, tail, B->A->A)', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'B', action = 'A', action = 'A') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;

INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:01',10,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:02',10,'B');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:02',10,NULL);
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:03',10,'C');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:04',10,'D');

SELECT '(forward, head, A) id >= 10', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'A') AS next_node FROM test_sequenceNextNode_Nullable WHERE id >= 10 GROUP BY id ORDER BY id;
SELECT '(forward, head, A) id >= 10', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode_Nullable WHERE id >= 10 GROUP BY id ORDER BY id;
SELECT '(forward, head, A) id >= 10', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'B', action = 'C') AS next_node FROM test_sequenceNextNode_Nullable WHERE id >= 10 GROUP BY id ORDER BY id;
SELECT '(forward, head, A) id >= 10', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'C') AS next_node FROM test_sequenceNextNode_Nullable WHERE id >= 10 GROUP BY id ORDER BY id;
SELECT '(backward, tail, A) id >= 10', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'D', action = 'C') AS next_node FROM test_sequenceNextNode_Nullable WHERE id >= 10 GROUP BY id ORDER BY id;
SELECT '(backward, tail, A) id >= 10', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'D', action = 'C', action = 'B') AS next_node FROM test_sequenceNextNode_Nullable WHERE id >= 10 GROUP BY id ORDER BY id;

INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:01',11,'A');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:01',11,'B');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:01',11,'C');
INSERT INTO test_sequenceNextNode_Nullable values ('2000-01-02 09:00:01',11,'D');

SELECT '(0, A) id = 11', count() FROM (SELECT id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'A') AS next_node FROM test_sequenceNextNode_Nullable WHERE id = 11 GROUP BY id HAVING next_node  = 'B');
SELECT '(0, C) id = 11', count() FROM (SELECT id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'C') AS next_node FROM test_sequenceNextNode_Nullable WHERE id = 11 GROUP BY id HAVING next_node = 'D');
SELECT '(0, B->C) id = 11', count() FROM (SELECT id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'B', action ='C') AS next_node FROM test_sequenceNextNode_Nullable WHERE id = 11 GROUP BY id HAVING next_node = 'D');
SELECT '(0, A->B->C) id = 11', count() FROM (SELECT id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'A', action = 'B', action = 'C') AS next_node FROM test_sequenceNextNode_Nullable WHERE id = 11 GROUP BY id HAVING next_node = 'D');
SELECT '(0, A) id = 11', count() FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'A') AS next_node FROM test_sequenceNextNode_Nullable WHERE id = 11 GROUP BY id HAVING next_node is NULL);
SELECT '(0, C) id = 11', count() FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'C') AS next_node FROM test_sequenceNextNode_Nullable WHERE id = 11 GROUP BY id HAVING next_node = 'B');
SELECT '(0, C->B) id = 11', count() FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'C', action ='B') AS next_node FROM test_sequenceNextNode_Nullable WHERE id = 11 GROUP BY id HAVING next_node = 'A');
SELECT '(0, C->B->A) id = 11', count() FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'C', action = 'B', action = 'A') AS next_node FROM test_sequenceNextNode_Nullable WHERE id = 11 GROUP BY id HAVING next_node is null);

SELECT '(forward, head) id < 10', id, sequenceNextNode('forward', 'head')(dt, action, 1) AS next_node FROM test_sequenceNextNode_Nullable WHERE id < 10 GROUP BY id ORDER BY id;
SELECT '(backward, tail) id < 10', id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM test_sequenceNextNode_Nullable WHERE id < 10 GROUP BY id ORDER BY id;

SELECT '(forward, first_match, A)', id, sequenceNextNode('forward', 'first_match')(dt, action, 1, action = 'A') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(forward, first_match, A->B)', id, sequenceNextNode('forward', 'first_match')(dt, action, 1, action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(forward, first_match, A->B->C)', id, sequenceNextNode('forward', 'first_match')(dt, action, 1, action = 'A', action = 'B', action = 'C') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(forward, first_match, B)', id, sequenceNextNode('forward', 'first_match')(dt, action, 1, action = 'B') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(forward, first_match, B->B)', id, sequenceNextNode('forward', 'first_match')(dt, action, 1, action = 'B', action = 'B') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(forward, first_match, B->A)', id, sequenceNextNode('forward', 'first_match')(dt, action, 1, action = 'B', action = 'A') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;

SELECT '(backward, first_match, A)', id, sequenceNextNode('backward', 'first_match')(dt, action, 1, action = 'A') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(backward, first_match, B)', id, sequenceNextNode('backward', 'first_match')(dt, action, 1, action = 'B') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(backward, first_match, B->A)', id, sequenceNextNode('backward', 'first_match')(dt, action, 1, action = 'B', action = 'A') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;
SELECT '(backward, first_match, B->B)', id, sequenceNextNode('backward', 'first_match')(dt, action, 1, action = 'B', action = 'B') AS next_node FROM test_sequenceNextNode_Nullable GROUP BY id ORDER BY id;

DROP TABLE IF EXISTS test_sequenceNextNode_Nullable;

-- The same testcases for a non-null type.

DROP TABLE IF EXISTS test_sequenceNextNode;

CREATE TABLE IF NOT EXISTS test_sequenceNextNode (dt DateTime, id int, action String) ENGINE = MergeTree() PARTITION BY dt ORDER BY id;

INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:01',1,'A');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:02',1,'B');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:03',1,'C');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:04',1,'D');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:01',2,'B');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:02',2,'B');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:03',2,'D');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:04',2,'C');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:01',3,'A');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:02',3,'B');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:01',4,'A');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:02',4,'A');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:03',4,'A');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:04',4,'B');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:05',4,'C');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:01',5,'A');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:02',5,'B');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:03',5,'A');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:04',5,'C');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:01',6,'A');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:02',6,'B');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:03',6,'A');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:04',6,'B');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:05',6,'C');

SELECT '(forward, head, A)', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'A') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(forward, head, B)', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'B') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(forward, head, C)', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'C') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(forward, head, D)', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'D') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(forward, head, E)', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'E') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(backward, tail, A)', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'A') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(backward, tail, B)', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'B') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(backward, tail, C)', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'C') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(backward, tail, D)', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'D') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(backward, tail, E)', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'E') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(forward, head, A->B)', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(forward, head, A->C)', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'A', action = 'C') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(forward, head, B->A)', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'B', action = 'A') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(backward, tail, A->B)', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(backward, tail, A->C)', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'A', action = 'C') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(backward, tail, B->A)', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'B', action = 'A') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(forward, head, A->A->B)', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'A', action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(forward, head, B->A->A)', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'B', action = 'A', action = 'A') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(backward, tail, A->A->B)', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'A', action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(backward, tail, B->A->A)', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'B', action = 'A', action = 'A') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;

INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:01',10,'A');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:02',10,'B');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:03',10,'C');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:04',10,'D');

SELECT '(forward, head, A) id >= 10', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'A') AS next_node FROM test_sequenceNextNode WHERE id >= 10 GROUP BY id ORDER BY id;
SELECT '(forward, head, A) id >= 10', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode WHERE id >= 10 GROUP BY id ORDER BY id;
SELECT '(forward, head, A) id >= 10', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'B', action = 'C') AS next_node FROM test_sequenceNextNode WHERE id >= 10 GROUP BY id ORDER BY id;
SELECT '(forward, head, A) id >= 10', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'C') AS next_node FROM test_sequenceNextNode WHERE id >= 10 GROUP BY id ORDER BY id;
SELECT '(backward, tail, A) id >= 10', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'D', action = 'C') AS next_node FROM test_sequenceNextNode WHERE id >= 10 GROUP BY id ORDER BY id;
SELECT '(backward, tail, A) id >= 10', id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'D', action = 'C', action = 'B') AS next_node FROM test_sequenceNextNode WHERE id >= 10 GROUP BY id ORDER BY id;

INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:01',11,'A');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:01',11,'B');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:01',11,'C');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:01',11,'D');

SELECT '(0, A) id = 11', count() FROM (SELECT id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'A') AS next_node FROM test_sequenceNextNode WHERE id = 11 GROUP BY id HAVING next_node  = 'B');
SELECT '(0, C) id = 11', count() FROM (SELECT id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'C') AS next_node FROM test_sequenceNextNode WHERE id = 11 GROUP BY id HAVING next_node = 'D');
SELECT '(0, B->C) id = 11', count() FROM (SELECT id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'B', action ='C') AS next_node FROM test_sequenceNextNode WHERE id = 11 GROUP BY id HAVING next_node = 'D');
SELECT '(0, A->B->C) id = 11', count() FROM (SELECT id, sequenceNextNode('forward', 'head')(dt, action, 1, action = 'A', action = 'B', action = 'C') AS next_node FROM test_sequenceNextNode WHERE id = 11 GROUP BY id HAVING next_node = 'D');
SELECT '(0, A) id = 11', count() FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'A') AS next_node FROM test_sequenceNextNode WHERE id = 11 GROUP BY id HAVING next_node is NULL);
SELECT '(0, C) id = 11', count() FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'C') AS next_node FROM test_sequenceNextNode WHERE id = 11 GROUP BY id HAVING next_node = 'B');
SELECT '(0, C->B) id = 11', count() FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'C', action ='B') AS next_node FROM test_sequenceNextNode WHERE id = 11 GROUP BY id HAVING next_node = 'A');
SELECT '(0, C->B->A) id = 11', count() FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1, action = 'C', action = 'B', action = 'A') AS next_node FROM test_sequenceNextNode WHERE id = 11 GROUP BY id HAVING next_node is null);

SELECT '(forward, head) id < 10', id, sequenceNextNode('forward', 'head')(dt, action, 1) AS next_node FROM test_sequenceNextNode WHERE id < 10 GROUP BY id ORDER BY id;
SELECT '(backward, tail) id < 10', id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM test_sequenceNextNode WHERE id < 10 GROUP BY id ORDER BY id;

SELECT '(forward, first_match, A)', id, sequenceNextNode('forward', 'first_match')(dt, action, 1, action = 'A') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(forward, first_match, A->B)', id, sequenceNextNode('forward', 'first_match')(dt, action, 1, action = 'A', action = 'B') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(forward, first_match, A->B->C)', id, sequenceNextNode('forward', 'first_match')(dt, action, 1, action = 'A', action = 'B', action = 'C') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(forward, first_match, B)', id, sequenceNextNode('forward', 'first_match')(dt, action, 1, action = 'B') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(forward, first_match, B->B)', id, sequenceNextNode('forward', 'first_match')(dt, action, 1, action = 'B', action = 'B') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(forward, first_match, B->A)', id, sequenceNextNode('forward', 'first_match')(dt, action, 1, action = 'B', action = 'A') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;

SELECT '(backward, first_match, A)', id, sequenceNextNode('backward', 'first_match')(dt, action, 1, action = 'A') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(backward, first_match, B)', id, sequenceNextNode('backward', 'first_match')(dt, action, 1, action = 'B') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(backward, first_match, B->A)', id, sequenceNextNode('backward', 'first_match')(dt, action, 1, action = 'B', action = 'A') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;
SELECT '(backward, first_match, B->B)', id, sequenceNextNode('backward', 'first_match')(dt, action, 1, action = 'B', action = 'B') AS next_node FROM test_sequenceNextNode GROUP BY id ORDER BY id;

SELECT '(max_args)', id, sequenceNextNode('forward', 'head')(dt, action, 1, action = '0', action = '1', action = '2', action = '3', action = '4', action = '5', action = '6', action = '7', action = '8', action = '9', action = '10', action = '11', action = '12', action = '13', action = '14', action = '15', action = '16', action = '17', action = '18', action = '19', action = '20', action = '21', action = '22', action = '23', action = '24', action = '25', action = '26', action = '27', action = '28', action = '29', action = '30', action = '31', action = '32', action = '33', action = '34', action = '35', action = '36', action = '37', action = '38', action = '39', action = '40', action = '41', action = '42', action = '43', action = '44', action = '45', action = '46', action = '47', action = '48', action = '49', action = '50', action = '51', action = '52', action = '53', action = '54', action = '55', action = '56', action = '57', action = '58', action = '59', action = '60', action = '61', action = '62', action = '63') from test_sequenceNextNode GROUP BY id ORDER BY id;

INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:01',12,'A');
INSERT INTO test_sequenceNextNode values ('2000-01-02 09:00:01',12,'A');

SELECT '(forward, head, A) id = 12', sequenceNextNode('forward', 'head')(dt, action, 1, action = 'A') AS next_node FROM test_sequenceNextNode WHERE id = 12;

DROP TABLE IF EXISTS test_sequenceNextNode;

DROP TABLE IF EXISTS test_base_condition;

CREATE TABLE IF NOT EXISTS test_base_condition (dt DateTime, id int, action String, referrer String) ENGINE = MergeTree() PARTITION BY dt ORDER BY id;

INSERT INTO test_base_condition values ('2000-01-02 09:00:01',1,'A','1');
INSERT INTO test_base_condition values ('2000-01-02 09:00:02',1,'B','2');
INSERT INTO test_base_condition values ('2000-01-02 09:00:03',1,'C','3');
INSERT INTO test_base_condition values ('2000-01-02 09:00:04',1,'D','4');
INSERT INTO test_base_condition values ('2000-01-02 09:00:01',2,'D','4');
INSERT INTO test_base_condition values ('2000-01-02 09:00:02',2,'C','3');
INSERT INTO test_base_condition values ('2000-01-02 09:00:03',2,'B','2');
INSERT INTO test_base_condition values ('2000-01-02 09:00:04',2,'A','1');
INSERT INTO test_base_condition values ('2000-01-02 09:00:01',3,'B','10');
INSERT INTO test_base_condition values ('2000-01-02 09:00:02',3,'B','2');
INSERT INTO test_base_condition values ('2000-01-02 09:00:03',3,'D','3');
INSERT INTO test_base_condition values ('2000-01-02 09:00:04',3,'C','4');

SELECT '(forward, head, 1)', id, sequenceNextNode('forward', 'head')(dt, action, referrer = '1') AS next_node FROM test_base_condition GROUP BY id ORDER BY id;
SELECT '(forward, head, 1, A)', id, sequenceNextNode('forward', 'head')(dt, action, referrer = '1', action = 'A') AS next_node FROM test_base_condition GROUP BY id ORDER BY id;
SELECT '(forward, head, 1, A->B)', id, sequenceNextNode('forward', 'head')(dt, action, referrer = '1', action = 'A', action = 'B') AS next_node FROM test_base_condition GROUP BY id ORDER BY id;

SELECT '(backward, tail, 1)', id, sequenceNextNode('backward', 'tail')(dt, action, referrer = '1') AS next_node FROM test_base_condition GROUP BY id ORDER BY id;
SELECT '(backward, tail, 1, A)', id, sequenceNextNode('backward', 'tail')(dt, action, referrer = '1', action = 'A') AS next_node FROM test_base_condition GROUP BY id ORDER BY id;
SELECT '(backward, tail, 1, A->B)', id, sequenceNextNode('backward', 'tail')(dt, action, referrer = '1', action = 'A', action = 'B') AS next_node FROM test_base_condition GROUP BY id ORDER BY id;

SELECT '(forward, first_match, 1, B)', id, sequenceNextNode('forward', 'first_match')(dt, action, referrer = '2', action = 'B') AS next_node FROM test_base_condition GROUP BY id ORDER BY id;
SELECT '(forward, first_match, 1, B->C)', id, sequenceNextNode('forward', 'first_match')(dt, action, referrer = '2', action = 'B', action = 'C') AS next_node FROM test_base_condition GROUP BY id ORDER BY id;

SELECT '(backward, first_match, 1, B)', id, sequenceNextNode('backward', 'first_match')(dt, action, referrer = '2', action = 'B') AS next_node FROM test_base_condition GROUP BY id ORDER BY id;
SELECT '(backward, first_match, 1, B->C)', id, sequenceNextNode('backward', 'first_match')(dt, action, referrer = '2', action = 'B', action = 'A') AS next_node FROM test_base_condition GROUP BY id ORDER BY id;

SET allow_experimental_funnel_functions = 0;
SELECT '(backward, first_match, 1, B->C)', id, sequenceNextNode('backward', 'first_match')(dt, action, referrer = '2', action = 'B', action = 'A') AS next_node FROM test_base_condition GROUP BY id ORDER BY id; -- { serverError 63 }

DROP TABLE IF EXISTS test_base_condition;
