-- Tags: no-object-storage, no-replicated-database

DROP TABLE IF EXISTS test;

EXPLAIN SYNTAX
CREATE TABLE test (a Int32)
ENGINE = MergeTree() order by tuple()
SETTINGS disk = disk(type=local, path='/var/lib/clickhouse/disks/local/');

CREATE TABLE test (a Int32)
ENGINE = MergeTree() order by tuple()
SETTINGS disk = disk(type=local, path='/local/'); -- { serverError BAD_ARGUMENTS }

CREATE TABLE test (a Int32)
ENGINE = MergeTree() order by tuple()
SETTINGS disk = disk(type=local, path='/var/lib/clickhouse/disks/local/');

INSERT INTO test SELECT number FROM numbers(100);
SELECT count() FROM test;

DETACH TABLE test;
ATTACH TABLE test;

SHOW CREATE TABLE test;
DESCRIBE TABLE test;

INSERT INTO test SELECT number FROM numbers(100);
SELECT count() FROM test;
