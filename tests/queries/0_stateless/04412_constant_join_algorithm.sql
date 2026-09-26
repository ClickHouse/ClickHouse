SET enable_analyzer = 1; -- 1 = 1 join condition causes troubles for old analyzer

DROP TABLE IF EXISTS constant_join_algorithm_t1;
DROP TABLE IF EXISTS constant_join_algorithm_t2;

CREATE TABLE constant_join_algorithm_t1 (id Int) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE constant_join_algorithm_t2 (id Int) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO constant_join_algorithm_t1 VALUES (1), (2);
INSERT INTO constant_join_algorithm_t2 VALUES (2), (3);

-- Constant joins use ConstantJoin independently of join_algorithm.
-- { echo on }
SELECT * FROM constant_join_algorithm_t1 AS t1 JOIN constant_join_algorithm_t2 AS t2 ON 1 = 1 ORDER BY ALL SETTINGS join_algorithm = 'full_sorting_merge';
SELECT * FROM constant_join_algorithm_t1 AS t1 JOIN constant_join_algorithm_t2 AS t2 ON 1 = 1 ORDER BY ALL SETTINGS join_algorithm = 'partial_merge';
SELECT * FROM constant_join_algorithm_t1 AS t1 JOIN constant_join_algorithm_t2 AS t2 ON 1 = 1 ORDER BY ALL SETTINGS join_algorithm = 'auto';

SELECT * FROM constant_join_algorithm_t1 AS t1 JOIN constant_join_algorithm_t2 AS t2 ON NULL ORDER BY ALL SETTINGS join_algorithm = 'full_sorting_merge';
SELECT * FROM constant_join_algorithm_t1 AS t1 JOIN constant_join_algorithm_t2 AS t2 ON NULL ORDER BY ALL SETTINGS join_algorithm = 'partial_merge';
SELECT * FROM constant_join_algorithm_t1 AS t1 LEFT JOIN constant_join_algorithm_t2 AS t2 ON NULL ORDER BY ALL SETTINGS join_algorithm = 'partial_merge';
SELECT * FROM constant_join_algorithm_t1 AS t1 RIGHT JOIN constant_join_algorithm_t2 AS t2 ON NULL ORDER BY ALL SETTINGS join_algorithm = 'auto';
SELECT * FROM constant_join_algorithm_t1 AS t1 FULL JOIN constant_join_algorithm_t2 AS t2 ON NULL ORDER BY ALL SETTINGS join_algorithm = 'partial_merge';

DROP TABLE IF EXISTS constant_join_algorithm_t1;
DROP TABLE IF EXISTS constant_join_algorithm_t2;
