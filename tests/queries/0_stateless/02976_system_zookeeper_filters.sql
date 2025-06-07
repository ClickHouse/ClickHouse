-- Tags: zookeeper, no-parallel, no-fasttest, long

SET allow_unrestricted_reads_from_keeper = 'false';

SELECT count() > 0 FROM system.zookeeper; -- { serverError BAD_ARGUMENTS }
SELECT count() > 0 FROM system.zookeeper WHERE name LIKE '%_%'; -- { serverError BAD_ARGUMENTS }
SELECT count() > 0 FROM system.zookeeper WHERE value LIKE '%'; -- { serverError BAD_ARGUMENTS }
SELECT count() > 0 FROM system.zookeeper WHERE path LIKE '/%'; -- { serverError BAD_ARGUMENTS }
SELECT count() > 0 FROM system.zookeeper WHERE path = '/';

SET allow_unrestricted_reads_from_keeper = 'true';

SELECT count() > 0 FROM system.zookeeper;
SELECT count() > 0 FROM system.zookeeper WHERE name LIKE '%_%';
SELECT count() > 0 FROM system.zookeeper WHERE value LIKE '%';
SELECT count() > 0 FROM system.zookeeper WHERE path LIKE '/%';
SELECT count() > 0 FROM system.zookeeper WHERE path = '/';
