#!/usr/bin/env bash
# Test: system.role_grants when a role is granted to another role.
# StorageSystemRoleGrants.fillData (src/Storages/System/StorageSystemRoleGrants.cpp)
# has two grantee type branches: USER (lines 72-77, always taken in CI) and ROLE
# (lines 78-84, never taken in CI). All existing tests only query role_grants for
# users (WHERE user_name = ...). Granting a role to a role (GRANT r1 TO r2) produces
# a row where the grantee_type == AccessEntityType::ROLE, exercising the ROLE branch
# that populates role_name and leaves user_name as NULL.
# Also exercises line 107: when iterating a user's granted roles, granted_role_is_default
# checks whether the role appears in the user's DEFAULT ROLE list (default_roles->match()).
# Users with DEFAULT ROLE NONE have a non-empty default_roles set that does not match
# any granted role, driving the match() call that was previously short-circuited.
#
# Roles and users are global. The names carry the test database, so parallel
# copies of this test (the CI flaky check) never see each other's entities, and
# the output prints comparisons instead of the names to keep the reference stable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PARENT="r_${CLICKHOUSE_DATABASE}_parent"
CHILD="r_${CLICKHOUSE_DATABASE}_child"
USR="u_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
DROP ROLE IF EXISTS ${PARENT};
DROP ROLE IF EXISTS ${CHILD};
DROP USER IF EXISTS ${USR};

CREATE ROLE ${CHILD};
CREATE ROLE ${PARENT};

-- Grant role to role: exercises the ROLE grantee branch (lines 78-84).
GRANT ${CHILD} TO ${PARENT};

SELECT
    role_name IS NOT NULL                 AS is_role_grantee,
    user_name IS NULL                     AS user_name_is_null,
    granted_role_name = '${CHILD}'        AS granted_is_child,
    granted_role_is_default
FROM system.role_grants
WHERE role_name = '${PARENT}'
ORDER BY granted_role_name;

-- Grant role to user with DEFAULT ROLE NONE: exercises default_roles->match()
-- at line 107 (the user has a non-null RolesOrUsersSet that does not match the
-- granted role, so granted_role_is_default = 0).
CREATE USER ${USR} DEFAULT ROLE NONE;
GRANT ${PARENT} TO ${USR};

SELECT
    user_name IS NOT NULL                 AS is_user_grantee,
    granted_role_name = '${PARENT}'       AS granted_is_parent,
    granted_role_is_default
FROM system.role_grants
WHERE user_name = '${USR}'
ORDER BY granted_role_name;

DROP USER ${USR};
DROP ROLE ${PARENT};
DROP ROLE ${CHILD};
"
