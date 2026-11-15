-- Regression test for issue #107530: context-dependent functions such as currentUser()
-- must evaluate to the inserting user (not an empty string) on the async_insert flush path,
-- both in DEFAULT/MATERIALIZED column expressions and in materialized views.

DROP TABLE IF EXISTS t_async_user;
DROP TABLE IF EXISTS t_async_user_src;
DROP TABLE IF EXISTS t_async_user_dst;
DROP VIEW IF EXISTS t_async_user_mv;

-- DEFAULT / MATERIALIZED column expressions calling currentUser().
CREATE TABLE t_async_user
(
    x Int32,
    u_default String DEFAULT currentUser(),
    u_materialized String MATERIALIZED currentUser()
)
ENGINE = MergeTree ORDER BY x;

INSERT INTO t_async_user (x) SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES (1);
INSERT INTO t_async_user (x) SETTINGS async_insert = 0                            VALUES (2);

-- The async row (x=1) and the sync row (x=2) must both record the inserting user.
-- Before the fix the async row recorded an empty string.
SELECT x, u_default = currentUser() AS default_matches, length(u_default) > 0 AS default_non_empty
FROM t_async_user ORDER BY x;
SELECT x, u_materialized = currentUser() AS materialized_matches, length(u_materialized) > 0 AS materialized_non_empty
FROM t_async_user ORDER BY x;

-- Materialized view selecting currentUser().
CREATE TABLE t_async_user_src (x Int32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_async_user_dst (x Int32, u String) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW t_async_user_mv TO t_async_user_dst AS SELECT x, currentUser() AS u FROM t_async_user_src;

INSERT INTO t_async_user_src SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES (10);
INSERT INTO t_async_user_src SETTINGS async_insert = 0                            VALUES (20);

SELECT x, u = currentUser() AS mv_matches, length(u) > 0 AS mv_non_empty
FROM t_async_user_dst ORDER BY x;

DROP VIEW t_async_user_mv;
DROP TABLE t_async_user_dst;
DROP TABLE t_async_user_src;
DROP TABLE t_async_user;
