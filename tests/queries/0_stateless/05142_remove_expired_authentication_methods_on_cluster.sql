-- Tags: zookeeper, no-fasttest, no-parallel, no-replicated-database
-- Tag no-replicated-database: ON CLUSTER is rewritten away for replicated access storage.
-- Tag no-parallel: creates a globally-named user.

-- An `ON CLUSTER` query is distributed as text, so `REMOVE EXPIRED AUTHENTICATION METHODS` has to survive
-- being formatted back and re-parsed on the remote node. Each node then evaluates the deadlines against
-- its own copy of the user, which is what makes the clause safe to distribute: no per-node state is
-- resolved on the initiator.

SET distributed_ddl_output_mode = 'none';

DROP USER IF EXISTS user_05142_remove_expired ON CLUSTER test_shard_localhost;

CREATE USER user_05142_remove_expired ON CLUSTER test_shard_localhost
    IDENTIFIED WITH plaintext_password BY 'a' VALID UNTIL '2020-01-01 00:00:00 UTC';
ALTER USER user_05142_remove_expired ON CLUSTER test_shard_localhost
    REMOVE EXPIRED AUTHENTICATION METHODS
    ADD IDENTIFIED WITH plaintext_password BY 'b' VALID UNTIL '2100-01-01 00:00:00 UTC';
SELECT arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = 'user_05142_remove_expired';

DROP USER user_05142_remove_expired ON CLUSTER test_shard_localhost;
