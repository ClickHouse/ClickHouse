-- Tags: no-fasttest, no-parallel, no-replicated-database
-- Test: exercises `throwIfWindowViewIsDisabled` paths in `StorageWindowView`
-- Covers: src/Storages/WindowView/StorageWindowView.cpp
--   line 1797 — throwIfWindowViewIsDisabled definition (member-flag and per-query setting branch)
--   line 1265 — constructor CREATE-mode check
--   line 467  — optimize()
--   line 478  — alter()
--   line 538  — checkAlterIsPossible()
--   line 1178 — read()
-- These error paths block window view operations when the analyzer setting is on, so the
-- server does not crash on startup and operations on a window view loaded in disabled state fail clearly.

DROP TABLE IF EXISTS wv_src_4202;
DROP TABLE IF EXISTS wv_4202;

SET allow_experimental_window_view = 1;
SET allow_experimental_analyzer = 0;
CREATE TABLE wv_src_4202 (a Int32, ts DateTime) ENGINE = MergeTree ORDER BY tuple();

-- 1) CREATE WINDOW VIEW with analyzer enabled must fail (constructor check at line 1265).
SET allow_experimental_analyzer = 1;
CREATE WINDOW VIEW wv_4202 ENGINE Memory AS
    SELECT count(a) AS cnt, tumbleStart(wid) AS w_start
    FROM wv_src_4202
    GROUP BY tumble(ts, INTERVAL '5' SECOND) AS wid; -- { serverError UNSUPPORTED_METHOD }

-- 2) Create the window view with analyzer disabled, then exercise per-query analyzer setting.
SET allow_experimental_analyzer = 0;
CREATE WINDOW VIEW wv_4202 ENGINE Memory AS
    SELECT count(a) AS cnt, tumbleStart(wid) AS w_start
    FROM wv_src_4202
    GROUP BY tumble(ts, INTERVAL '5' SECOND) AS wid;

-- read() throw path (line 1178) — query context has analyzer=1.
SELECT * FROM wv_4202 SETTINGS allow_experimental_analyzer = 1; -- { serverError UNSUPPORTED_METHOD }

-- optimize() throw path (line 467).
OPTIMIZE TABLE wv_4202 SETTINGS allow_experimental_analyzer = 1; -- { serverError UNSUPPORTED_METHOD }

-- alter() throw path (lines 478, 538) — set analyzer flag at session level.
SET allow_experimental_analyzer = 1;
ALTER TABLE wv_4202 MODIFY QUERY
    SELECT count(a) AS cnt, tumbleStart(wid) AS w_start
    FROM wv_src_4202
    GROUP BY tumble(ts, INTERVAL '10' SECOND) AS wid; -- { serverError UNSUPPORTED_METHOD }
SET allow_experimental_analyzer = 0;

-- 3) Re-attach with analyzer enabled puts the table in disabled_due_to_analyzer state
--    (line 1262); SELECT then fails even with analyzer=0 in the query context (member-flag branch at line 1799).
DETACH TABLE wv_4202;
SET allow_experimental_analyzer = 1;
ATTACH TABLE wv_4202;
SET allow_experimental_analyzer = 0;
SELECT * FROM wv_4202; -- { serverError UNSUPPORTED_METHOD }

DROP TABLE wv_4202;
DROP TABLE wv_src_4202;
