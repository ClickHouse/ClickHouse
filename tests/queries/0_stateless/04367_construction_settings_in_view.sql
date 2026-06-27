-- A view created `AS SELECT ... SETTINGS <construction setting>` keeps the setting verbatim in its
-- stored definition (so `SHOW CREATE` shows it) and applies it on read: the construction settings are
-- materialized on the view's inner query, the same way they are for a directly executed query. This
-- covers the stored-view lifecycle gap — the inner query bypasses `executeQuery`'s wrapping, so it is
-- materialized in the `StorageView` constructor instead.

DROP TABLE IF EXISTS t_src;
DROP VIEW IF EXISTS v_limit;
DROP VIEW IF EXISTS v_filter;
DROP VIEW IF EXISTS v_order;

CREATE TABLE t_src (x UInt64) ENGINE = MergeTree ORDER BY x AS SELECT number FROM numbers(10);

SELECT '-- `limit` in the view definition caps the view result (3 rows)';
CREATE VIEW v_limit AS SELECT x FROM t_src ORDER BY x SETTINGS limit = 3;
SELECT count() FROM v_limit;

SELECT '-- the construction setting is preserved verbatim in SHOW CREATE';
SHOW CREATE VIEW v_limit FORMAT TSVRaw;

SELECT '-- `filter` in the view definition filters the view result (x >= 7)';
CREATE VIEW v_filter AS SELECT x FROM t_src SETTINGS filter = 'x >= 7';
SELECT count() FROM v_filter;
SELECT x FROM v_filter ORDER BY x;

SELECT '-- `sort` + `limit` in the view definition select the top rows (the two largest)';
CREATE VIEW v_order AS SELECT x FROM t_src SETTINGS sort = '-x', limit = 2;
SELECT x FROM v_order ORDER BY x;

SELECT '-- the construction setting also applies through the analyzer view inlining';
SELECT count() FROM v_limit SETTINGS allow_experimental_analyzer = 1, analyzer_inline_views = 1;

DROP VIEW v_order;
DROP VIEW v_filter;
DROP VIEW v_limit;
DROP TABLE t_src;
