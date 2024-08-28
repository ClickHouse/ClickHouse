-- Tags: no-fasttest, no-parallel, no-replicated-database
-- Tag no-replicated-database: https://s3.amazonaws.com/clickhouse-test-reports/65277/43e9a7ba4bbf7f20145531b384a31304895b55bc/stateless_tests__release__old_analyzer__s3__databasereplicated__[1_2].html and https://github.com/ClickHouse/ClickHouse/blob/011c694117845500c82f9563c65930429979982f/tests/queries/0_stateless/01175_distributed_ddl_output_mode_long.sh#L4

DROP USER IF EXISTS u_03174_no_login;

CREATE USER u_03174_no_login;

-- multiple identified with, not allowed
ALTER USER u_03174_no_login IDENTIFIED WITH plaintext_password by '7', IDENTIFIED plaintext_password by '8'; -- { clientError SYNTAX_ERROR }

-- CREATE Multiple identified with, not allowed
CREATE USER u_03174_no_login IDENTIFIED WITH plaintext_password by '7', IDENTIFIED WITH plaintext_password by '8'; -- { clientError SYNTAX_ERROR }

DROP USER u_03174_no_login;

-- Create user with no identification
CREATE USER u_03174_no_login;

-- Add identified with, should not be allowed because user is currently identified with no_password and it can not co-exist with other auth types
ALTER USER u_03174_no_login ADD IDENTIFIED WITH plaintext_password by '7'; -- { serverError BAD_ARGUMENTS }

-- Try to add no_password mixed with other authentication methods, should not be allowed
ALTER USER u_03174_no_login ADD IDENTIFIED WITH plaintext_password by '8', no_password; -- { clientError SYNTAX_ERROR }

-- Adding no_password, should fail
ALTER USER u_03174_no_login ADD IDENTIFIED WITH no_password; -- { clientError SYNTAX_ERROR }

DROP USER IF EXISTS u_03174_no_login;

-- Create user with mix both implicit and explicit auth type, starting with with
CREATE USER u_03174_no_login IDENTIFIED WITH plaintext_password by '1', by '2', bcrypt_password by '3', by '4';
SHOW CREATE USER u_03174_no_login;

DROP USER IF EXISTS u_03174_no_login;

-- Create user with mix both implicit and explicit auth type, starting with with. On cluster
CREATE USER u_03174_no_login ON CLUSTER test_shard_localhost IDENTIFIED WITH plaintext_password by '1', by '2', bcrypt_password by '3', by '4';
SHOW CREATE USER u_03174_no_login;

DROP USER IF EXISTS u_03174_no_login;

-- Create user with mix both implicit and explicit auth type, starting with by
CREATE USER u_03174_no_login IDENTIFIED by '1', plaintext_password by '2', bcrypt_password by '3', by '4';
SHOW CREATE USER u_03174_no_login;

DROP USER IF EXISTS u_03174_no_login;

-- Create user with mix both implicit and explicit auth type, starting with by. On cluster
CREATE USER u_03174_no_login ON CLUSTER test_shard_localhost IDENTIFIED by '1', plaintext_password by '2', bcrypt_password by '3', by '4';
SHOW CREATE USER u_03174_no_login;

DROP USER IF EXISTS u_03174_no_login;

-- Use WITH without providing authentication type, should fail
CREATE USER u_03174_no_login IDENTIFIED WITH BY '1'; -- { clientError SYNTAX_ERROR }

-- Create user with ADD identification, should fail, add is not allowed for create query
CREATE USER u_03174_no_login ADD IDENTIFIED WITH plaintext_password by '1'; -- { clientError SYNTAX_ERROR }

-- Trailing comma should result in syntax error
ALTER USER u_03174_no_login ADD IDENTIFIED WITH plaintext_password by '1',; -- { clientError SYNTAX_ERROR }

-- First auth method can't specify type if WITH keyword is not present
CREATE USER u_03174_no_login IDENTIFIED plaintext_password by '1'; -- { clientError SYNTAX_ERROR }

-- RESET AUTHENTICATION METHODS TO NEW can only be used on alter statement
CREATE USER u_03174_no_login RESET AUTHENTICATION METHODS TO NEW; -- { clientError SYNTAX_ERROR }

-- ADD NOT IDENTIFIED should result in syntax error
ALTER USER u_03174_no_login ADD NOT IDENTIFIED; -- { clientError SYNTAX_ERROR }

-- RESET AUTHENTICATION METHODS TO NEW cannot be used along with [ADD] IDENTIFIED clauses
ALTER USER u_03174_no_login IDENTIFIED WITH plaintext_password by '1' RESET AUTHENTICATION METHODS TO NEW; -- { clientError SYNTAX_ERROR }
