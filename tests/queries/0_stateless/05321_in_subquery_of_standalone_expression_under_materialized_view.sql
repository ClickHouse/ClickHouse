-- The `IN` subquery of a standalone expression - here a row policy, applied while a `Merge` table reads its child -
-- is planned by the analyzer as a query of its own. Under a materialized view it reads the source table from
-- the inserted block, as the rest of the view's query does. The source is a `Null` table, so the subquery
-- sees the inserted rows only if the view source is substituted.

DROP TABLE IF EXISTS in_sub_mv_view;
DROP TABLE IF EXISTS in_sub_mv_dst;
DROP TABLE IF EXISTS in_sub_mv_merge;
DROP TABLE IF EXISTS in_sub_mv_child;
DROP TABLE IF EXISTS in_sub_mv_src;

CREATE TABLE in_sub_mv_src (x UInt64) ENGINE = Null;
CREATE TABLE in_sub_mv_child (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO in_sub_mv_child VALUES (1), (2), (3);
CREATE ROW POLICY OR REPLACE in_sub_mv_policy_05321 ON in_sub_mv_child
    USING x IN (SELECT x FROM in_sub_mv_src) TO ALL;
CREATE TABLE in_sub_mv_merge (x UInt64) ENGINE = Merge(currentDatabase(), '^in_sub_mv_child$');
CREATE TABLE in_sub_mv_dst (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW in_sub_mv_view TO in_sub_mv_dst
    AS SELECT x FROM in_sub_mv_src WHERE x IN (SELECT x FROM in_sub_mv_merge);

INSERT INTO in_sub_mv_src VALUES (2), (3), (4);
SELECT x FROM in_sub_mv_dst ORDER BY x;

DROP ROW POLICY in_sub_mv_policy_05321 ON in_sub_mv_child;
DROP TABLE in_sub_mv_view;
DROP TABLE in_sub_mv_dst;
DROP TABLE in_sub_mv_merge;
DROP TABLE in_sub_mv_child;
DROP TABLE in_sub_mv_src;
