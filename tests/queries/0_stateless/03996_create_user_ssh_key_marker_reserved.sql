-- Test: rejects creating users whose name starts with the SSH key authentication marker.
-- Covers: src/Access/User.cpp:34 — name_.starts_with(SSH_KEY_AUTHENTICAION_MARKER) check.
-- Marker value (src/Core/Protocol.h:64): " SSH KEY AUTHENTICATION ".
-- Existing test 01119_weird_user_names.sql covers the parallel ` INTERSERVER SECRET ` marker
-- but does NOT cover the SSH KEY marker reserved-prefix check.
-- A regression here could allow creating a user whose name then collides with the
-- SSH-auth wire-protocol marker stripped by TCPHandler::receiveHello.

DROP USER IF EXISTS ` SSH KEY AUTHENTICATION `;
DROP USER IF EXISTS ` SSH KEY AUTHENTICATION foo`;

-- Exact marker as user name → reserved.
CREATE USER ` SSH KEY AUTHENTICATION `; -- { serverError BAD_ARGUMENTS }

-- Marker as a prefix of user name → reserved (starts_with check).
CREATE USER ` SSH KEY AUTHENTICATION foo`; -- { serverError BAD_ARGUMENTS }

SELECT 'ok';
