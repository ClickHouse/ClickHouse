-- Keep the cross product when the mutation runs through either analyzer.
SET join_algorithm = 'hash';
SET cross_to_inner_join_rewrite = 0;
SET mutations_sync = 2;

CREATE TABLE mutation_target (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO mutation_target VALUES (0, 0), (1, 0), (2, 0);
CREATE TABLE mutation_left (n UInt64) ENGINE = Memory;
INSERT INTO mutation_left VALUES (1), (2);
CREATE TABLE mutation_right (n UInt64) ENGINE = Memory;
INSERT INTO mutation_right VALUES (10), (20), (30);

ALTER TABLE mutation_target UPDATE v = (SELECT sum(l.n * r.n) FROM mutation_left AS l CROSS JOIN mutation_right AS r) WHERE k > 0;
SELECT k, v FROM mutation_target ORDER BY k;

DROP TABLE mutation_target;
DROP TABLE mutation_left;
DROP TABLE mutation_right;
