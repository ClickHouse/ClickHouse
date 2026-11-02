-- Tags: no-parallel, zookeeper
-- zookeeper: SYSTEM SYNC TRANSACTION LOG reads the transaction log from Keeper after access is granted.
-- no-parallel: creates a temporary user; avoid concurrent GRANT/DROP races on the same name pattern.

DROP USER IF EXISTS user_test_04651;
CREATE USER user_test_04651;

-- Before the fix, local SYSTEM SYNC TRANSACTION LOG never called checkAccess, so a user
-- without SYSTEM SYNC TRANSACTION LOG could still run the command (only the feature flag
-- allow_experimental_transactions was checked). ON CLUSTER already required the privilege.
-- Without the privilege, expect ACCESS_DENIED (checked before the transactions feature flag).
EXECUTE AS user_test_04651 SYSTEM SYNC TRANSACTION LOG; -- { serverError ACCESS_DENIED }

-- Grant the privilege that ON CLUSTER already required.
GRANT SYSTEM SYNC TRANSACTION LOG ON *.* TO user_test_04651;

-- With the privilege, the command must get past access control and sync the transaction log.
EXECUTE AS user_test_04651 SYSTEM SYNC TRANSACTION LOG;

DROP USER user_test_04651;
