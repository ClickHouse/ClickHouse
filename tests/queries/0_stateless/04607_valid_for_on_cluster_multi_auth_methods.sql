-- Tags: zookeeper, no-fasttest, no-parallel, no-replicated-database
-- Tag no-replicated-database: ON CLUSTER is rewritten away for replicated access storage.
-- Tag no-parallel: creates a globally-named user.

-- A user-level (global) `VALID UNTIL`/`VALID FOR` clause applies to every authentication method of the
-- user. When such a query is distributed `ON CLUSTER`, its text is formatted and re-parsed on every
-- replica, and the parser treats the clause as global only while no authentication method has been
-- parsed yet. The formatter therefore has to print the global clause before the `IDENTIFIED` list;
-- otherwise it would re-parse as the last method's clause and only that method would get the deadline.

SET distributed_ddl_output_mode = 'none';

DROP USER IF EXISTS user_04607_valid_for ON CLUSTER test_shard_localhost;

-- User-level VALID FOR combined with several explicit authentication methods: after the cluster
-- round-trip, all methods must carry the same resolved deadline.
CREATE USER user_04607_valid_for ON CLUSTER test_shard_localhost
    VALID FOR INTERVAL 1 DAY
    IDENTIFIED WITH plaintext_password BY 'a', plaintext_password BY 'b';
SELECT length(valid_until),
       valid_until[1] = valid_until[2],
       valid_until[1] BETWEEN now() + INTERVAL 23 HOUR AND now() + INTERVAL 25 HOUR
FROM system.users WHERE name = 'user_04607_valid_for';
DROP USER user_04607_valid_for ON CLUSTER test_shard_localhost;

-- Same for the absolute user-level VALID UNTIL form.
CREATE USER user_04607_valid_for ON CLUSTER test_shard_localhost
    VALID UNTIL '2035-01-01 00:00:00 UTC'
    IDENTIFIED WITH plaintext_password BY 'a', plaintext_password BY 'b';
SELECT length(valid_until),
       valid_until[1] = valid_until[2],
       toUInt32(valid_until[1])
FROM system.users WHERE name = 'user_04607_valid_for';
DROP USER user_04607_valid_for ON CLUSTER test_shard_localhost;

-- ALTER ... VALID FOR ... ADD IDENTIFIED: the global deadline must also apply to the method that
-- already existed on the replica before the ALTER.
CREATE USER user_04607_valid_for ON CLUSTER test_shard_localhost IDENTIFIED WITH plaintext_password BY 'a';
ALTER USER user_04607_valid_for ON CLUSTER test_shard_localhost
    VALID FOR INTERVAL 2 DAY
    ADD IDENTIFIED WITH plaintext_password BY 'b';
SELECT length(valid_until),
       valid_until[1] = valid_until[2],
       valid_until[1] BETWEEN now() + INTERVAL 47 HOUR AND now() + INTERVAL 49 HOUR
FROM system.users WHERE name = 'user_04607_valid_for';
DROP USER user_04607_valid_for ON CLUSTER test_shard_localhost;
