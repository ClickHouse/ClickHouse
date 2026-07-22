-- Tags: no-parallel

SET enable_analyzer = 1;
SET analyzer_inline_views = 1;

DROP TABLE IF EXISTS rp_src;
DROP VIEW IF EXISTS rp_view;
DROP VIEW IF EXISTS rp_view_tc;
DROP ROW POLICY IF EXISTS rp_pol ON rp_view;
DROP ROW POLICY IF EXISTS rp_pol_tc ON rp_view_tc;

CREATE TABLE rp_src (x Int32, y Int32) ENGINE = MergeTree ORDER BY x;
INSERT INTO rp_src VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);

--- Row policy on a simple view (column names match inner query).
CREATE VIEW rp_view AS SELECT x, y FROM rp_src;
CREATE ROW POLICY rp_pol ON rp_view USING x >= 3 TO ALL;

SELECT '--- inlined view with row policy';
SELECT x, y FROM rp_view ORDER BY x;

SELECT '--- same without inlining';
SELECT x, y FROM rp_view ORDER BY x SETTINGS analyzer_inline_views = 0;

--- Row policy on a view with type conversion (wider column types).
CREATE VIEW rp_view_tc (x Int64, y Int64) AS SELECT x, y FROM rp_src;
CREATE ROW POLICY rp_pol_tc ON rp_view_tc USING y <= 30 TO ALL;

SELECT '--- inlined view with row policy + type conversion';
SELECT x, y FROM rp_view_tc ORDER BY x;

SELECT '--- same without inlining';
SELECT x, y FROM rp_view_tc ORDER BY x SETTINGS analyzer_inline_views = 0;

DROP ROW POLICY rp_pol ON rp_view;
DROP ROW POLICY rp_pol_tc ON rp_view_tc;
DROP VIEW rp_view;
DROP VIEW rp_view_tc;
DROP TABLE rp_src;
