-- The target of `remote`/`cluster` may itself be a table function. Such a target is read-only:
-- there is no remote table to insert into, so an `INSERT` is rejected with `NOT_IMPLEMENTED` by
-- `StorageDistributed::write`. The structure probe that runs before that rejection must resolve
-- the target in read mode even when the query itself is an `INSERT` or a `CREATE TABLE ... AS`.

SELECT count() FROM cluster('test_shard_localhost', numbers(10));
SELECT count() FROM remote('127.0.0.1', numbers(10));

INSERT INTO FUNCTION cluster('test_shard_localhost', numbers(10)) VALUES (100); -- { serverError NOT_IMPLEMENTED }
INSERT INTO FUNCTION remote('127.0.0.1', numbers(10)) VALUES (100); -- { serverError NOT_IMPLEMENTED }
INSERT INTO FUNCTION cluster('test_shard_localhost', numbers(10)) SELECT * FROM numbers(5); -- { serverError NOT_IMPLEMENTED }

-- `CREATE TABLE ... AS remote(..., tf())` also executes the table function in insert mode; the
-- structure must still be inferred (in read mode) and the table created, after which it reads
-- fine and rejects an `INSERT` the same way.
DROP TABLE IF EXISTS table_over_remote_over_tf;
CREATE TABLE table_over_remote_over_tf AS remote('127.0.0.1', numbers(10));
SELECT count() FROM table_over_remote_over_tf;
INSERT INTO table_over_remote_over_tf VALUES (100); -- { serverError NOT_IMPLEMENTED }
DROP TABLE table_over_remote_over_tf;
