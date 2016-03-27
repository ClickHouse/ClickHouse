DROP TABLE IF EXISTS test.parallel_replicas;
DROP TABLE IF EXISTS test.parallel_replicas_backup;

CREATE TABLE test.parallel_replicas (d Date DEFAULT today(), x UInt32, u UInt64, s String) ENGINE = MergeTree(d, cityHash64(u, s), (x, d, cityHash64(u, s)), 8192);
INSERT INTO test.parallel_replicas (x, u, s) VALUES (1, 2, 'A'),(3, 4, 'B'),(5, 6, 'C'),(7, 8, 'D'),(9,10,'E');
INSERT INTO test.parallel_replicas (x, u, s) VALUES (11, 12, 'F'),(13, 14, 'G'),(15, 16, 'H'),(17, 18, 'I'),(19,20,'J');
INSERT INTO test.parallel_replicas (x, u, s) VALUES (21, 22, 'K'),(23, 24, 'L'),(25, 26, 'M'),(27, 28, 'N'),(29,30,'O');
INSERT INTO test.parallel_replicas (x, u, s) VALUES (31, 32, 'P'),(33, 34, 'Q'),(35, 36, 'R'),(37, 38, 'S'),(39,40,'T');
INSERT INTO test.parallel_replicas (x, u, s) VALUES (41, 42, 'U'),(43, 44, 'V'),(45, 46, 'W'),(47, 48, 'X'),(49,50,'Y');
INSERT INTO test.parallel_replicas (x, u, s) VALUES (51, 52, 'Z');

/*
 * Проверяем, что:
 * - на каждой реплике таблица не пустая;
 * - объединение данных всех реплик совпадает с содержанием таблицы test.parallel_replicas.
 */

/* Две реплики */

CREATE TABLE test.parallel_replicas_backup(d Date DEFAULT today(), x UInt32, u UInt64, s String) ENGINE = Memory;

SET parallel_replicas_count = 2;

SET parallel_replica_offset = 0;
INSERT INTO test.parallel_replicas_backup(d, x, u, s) SELECT d, x, u, s FROM test.parallel_replicas;
SELECT count() > 0 FROM test.parallel_replicas;

SET parallel_replica_offset = 1;
INSERT INTO test.parallel_replicas_backup(d, x, u, s) SELECT d, x, u, s FROM test.parallel_replicas;
SELECT count() > 0 FROM test.parallel_replicas;

SET parallel_replicas_count = 0;
SELECT x, u, s FROM test.parallel_replicas_backup ORDER BY x, u, s ASC;

DROP TABLE test.parallel_replicas_backup;
CREATE TABLE test.parallel_replicas_backup(d Date DEFAULT today(), x UInt32, u UInt64, s String) ENGINE = Memory;

/* Три реплики */

SET parallel_replicas_count = 3;

SET parallel_replica_offset = 0;
INSERT INTO test.parallel_replicas_backup(d, x, u, s) SELECT d, x, u, s FROM test.parallel_replicas;
SELECT count() > 0 FROM test.parallel_replicas;

SET parallel_replica_offset = 1;
INSERT INTO test.parallel_replicas_backup(d, x, u, s) SELECT d, x, u, s FROM test.parallel_replicas;
SELECT count() > 0 FROM test.parallel_replicas;

SET parallel_replica_offset = 2;
INSERT INTO test.parallel_replicas_backup(d, x, u, s) SELECT d, x, u, s FROM test.parallel_replicas;
SELECT count() > 0 FROM test.parallel_replicas;

SET parallel_replicas_count = 0;
SELECT x, u, s FROM test.parallel_replicas_backup ORDER BY x, u, s ASC;

DROP TABLE test.parallel_replicas;
DROP TABLE test.parallel_replicas_backup;
