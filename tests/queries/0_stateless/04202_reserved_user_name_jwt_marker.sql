-- Tags: no-parallel
-- Test: exercises `User::setName` reserved-name check for the JWT_AUTHENTICAION_MARKER prefix.
-- Covers: src/Access/User.cpp:36-37 - name_.starts_with(EncodedUserInfo::JWT_AUTHENTICAION_MARKER)
-- The marker " JWT AUTHENTICATION " (Core/Protocol.h:67) is sent by clickhouse-client as the
-- "user name" on the wire to signal JWT authentication. Allowing a real user to be created with
-- this name prefix would create a collision in the native protocol handshake (the same reason
-- USER_INTERSERVER_MARKER is reserved, already covered by 01119_weird_user_names.sql).

CREATE USER ' JWT AUTHENTICATION test_04202'; -- { serverError BAD_ARGUMENTS }
CREATE USER ' JWT AUTHENTICATION '; -- { serverError BAD_ARGUMENTS }
CREATE USER ' JWT AUTHENTICATION suffix_chars'; -- { serverError BAD_ARGUMENTS }

-- Confirm the marker check is prefix-based (starts_with), not exact match: a name that merely
-- contains the marker substring later in the string must still be accepted by setName.
-- This drives the actual User::setName path (negative branch of starts_with).
DROP USER IF EXISTS 'u_04202 JWT AUTHENTICATION y';
CREATE USER 'u_04202 JWT AUTHENTICATION y';
SELECT name FROM system.users WHERE name = 'u_04202 JWT AUTHENTICATION y';
DROP USER 'u_04202 JWT AUTHENTICATION y';
