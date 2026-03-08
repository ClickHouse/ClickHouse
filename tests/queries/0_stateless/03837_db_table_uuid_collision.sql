-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/98615
-- Creating a table with the same UUID as an existing database should give a proper error,
-- not trigger an assertion failure.

DROP DATABASE IF EXISTS d_uuid_collision_98615;

CREATE DATABASE d_uuid_collision_98615 UUID '10000000-0000-0000-0000-000000000001' ENGINE = Atomic;

-- Attempt to create a table with the same UUID as the database.
-- This should fail with TABLE_ALREADY_EXISTS, not abort the server.
CREATE TABLE d_uuid_collision_98615.t0 UUID '10000000-0000-0000-0000-000000000001' (c0 Int32) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError TABLE_ALREADY_EXISTS }

DROP DATABASE d_uuid_collision_98615;
