-- `sequenceNextNode` must sort events added to a state after it was already sorted by a merge.
-- The adaptive aggregator can add rows to a state that it merged into before. Every row is in its own part.

SET enable_funnel_functions = 1;
SET max_threads = 3, max_block_size = 66153, group_by_two_level_threshold = 166130, group_by_two_level_threshold_bytes = 1;

DROP TABLE IF EXISTS t_sequence_next_node;
CREATE TABLE t_sequence_next_node (dt DateTime, id Int32, action Nullable(String)) ENGINE = MergeTree PARTITION BY dt ORDER BY id;
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:01',1,'A');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:02',1,'B');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:03',1,'C');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:04',1,'D');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:01',2,'B');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:02',2,'B');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:03',2,'D');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:04',2,'C');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:01',3,'A');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:02',3,'B');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:01',4,'A');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:02',4,'A');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:03',4,'A');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:04',4,'B');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:05',4,'C');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:01',5,'A');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:02',5,'B');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:03',5,'A');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:04',5,'C');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:01',6,'A');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:02',6,'B');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:03',6,'A');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:04',6,'B');
INSERT INTO t_sequence_next_node VALUES ('2000-01-02 09:00:05',6,'C');

SELECT DISTINCT res FROM
(
    SELECT groupArray(next_node) AS res FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM t_sequence_next_node GROUP BY id ORDER BY id)
    UNION ALL
    SELECT groupArray(next_node) AS res FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM t_sequence_next_node GROUP BY id ORDER BY id)
    UNION ALL
    SELECT groupArray(next_node) AS res FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM t_sequence_next_node GROUP BY id ORDER BY id)
    UNION ALL
    SELECT groupArray(next_node) AS res FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM t_sequence_next_node GROUP BY id ORDER BY id)
    UNION ALL
    SELECT groupArray(next_node) AS res FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM t_sequence_next_node GROUP BY id ORDER BY id)
    UNION ALL
    SELECT groupArray(next_node) AS res FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM t_sequence_next_node GROUP BY id ORDER BY id)
    UNION ALL
    SELECT groupArray(next_node) AS res FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM t_sequence_next_node GROUP BY id ORDER BY id)
    UNION ALL
    SELECT groupArray(next_node) AS res FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM t_sequence_next_node GROUP BY id ORDER BY id)
    UNION ALL
    SELECT groupArray(next_node) AS res FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM t_sequence_next_node GROUP BY id ORDER BY id)
    UNION ALL
    SELECT groupArray(next_node) AS res FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM t_sequence_next_node GROUP BY id ORDER BY id)
    UNION ALL
    SELECT groupArray(next_node) AS res FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM t_sequence_next_node GROUP BY id ORDER BY id)
    UNION ALL
    SELECT groupArray(next_node) AS res FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM t_sequence_next_node GROUP BY id ORDER BY id)
    UNION ALL
    SELECT groupArray(next_node) AS res FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM t_sequence_next_node GROUP BY id ORDER BY id)
    UNION ALL
    SELECT groupArray(next_node) AS res FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM t_sequence_next_node GROUP BY id ORDER BY id)
    UNION ALL
    SELECT groupArray(next_node) AS res FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM t_sequence_next_node GROUP BY id ORDER BY id)
    UNION ALL
    SELECT groupArray(next_node) AS res FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM t_sequence_next_node GROUP BY id ORDER BY id)
    UNION ALL
    SELECT groupArray(next_node) AS res FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM t_sequence_next_node GROUP BY id ORDER BY id)
    UNION ALL
    SELECT groupArray(next_node) AS res FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM t_sequence_next_node GROUP BY id ORDER BY id)
    UNION ALL
    SELECT groupArray(next_node) AS res FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM t_sequence_next_node GROUP BY id ORDER BY id)
    UNION ALL
    SELECT groupArray(next_node) AS res FROM (SELECT id, sequenceNextNode('backward', 'tail')(dt, action, 1) AS next_node FROM t_sequence_next_node GROUP BY id ORDER BY id)
);

DROP TABLE t_sequence_next_node;
