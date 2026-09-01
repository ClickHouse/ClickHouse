-- Tags: no-parallel
-- Tag no-parallel: user-defined types live in a single process-wide namespace.

DROP TYPE IF EXISTS RollbackUserId;

CREATE TYPE RollbackUserId AS UInt64;

-- A `CREATE TYPE` that fails must not remove the type that is already registered under that name.
CREATE TYPE RollbackUserId AS String; -- { serverError TYPE_ALREADY_EXISTS }

SHOW TYPE RollbackUserId;
SELECT name, base_type_ast_string FROM system.user_defined_types WHERE name = 'RollbackUserId';

DROP TYPE RollbackUserId;
