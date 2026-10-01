-- Tags: zookeeper, no-fasttest, no-parallel, no-replicated-database
-- Tag no-replicated-database: ON CLUSTER is rewritten away for replicated access storage.
-- Tag no-parallel: creates a globally-named user.

-- `VALID FOR <interval>` is resolved to an absolute `VALID UNTIL` on the initiator before the query is
-- distributed `ON CLUSTER`, so every replica stores the same deadline instead of re-evaluating
-- `now + interval` against its own clock. Here we only check that the shortcut works end to end over the
-- cluster path (the absolute literal round-trips through serialization and re-parsing on the remote node).

SET distributed_ddl_output_mode = 'none';

DROP USER IF EXISTS user_04600_valid_for ON CLUSTER test_shard_localhost;

-- VALID FOR at the user level.
CREATE USER user_04600_valid_for ON CLUSTER test_shard_localhost VALID FOR INTERVAL 1 DAY;
SELECT valid_until[1] BETWEEN now() + INTERVAL 23 HOUR AND now() + INTERVAL 25 HOUR
FROM system.users WHERE name = 'user_04600_valid_for';
DROP USER user_04600_valid_for ON CLUSTER test_shard_localhost;

-- VALID FOR together with an authentication method.
CREATE USER user_04600_valid_for ON CLUSTER test_shard_localhost
    IDENTIFIED WITH plaintext_password BY 'x' VALID FOR INTERVAL 2 DAY;
SELECT valid_until[1] BETWEEN now() + INTERVAL 47 HOUR AND now() + INTERVAL 49 HOUR
FROM system.users WHERE name = 'user_04600_valid_for';
DROP USER user_04600_valid_for ON CLUSTER test_shard_localhost;
