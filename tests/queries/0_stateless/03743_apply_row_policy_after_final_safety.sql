-- Edge cases for row-policy / PREWHERE deferral with FINAL. Three things to guard:
--   1. SK-only-but-non-deterministic row policy must stay deferred
--   2. SK-only deterministic row policy without PREWHERE skips deferral
--   3. Non-SK row policy keeps PREWHERE deferred even when PREWHERE is
--      itself SK-only — otherwise `throwIf` leak hidden rows

SET enable_analyzer = 1;

SELECT '= non-deterministic row policy that structurally only uses SK columns must remain deferred =';

DROP TABLE IF EXISTS tab_nondet_policy;
DROP ROW POLICY IF EXISTS pol_nondet ON tab_nondet_policy;

CREATE TABLE tab_nondet_policy (x UInt32, y String, version UInt32)
ENGINE = ReplacingMergeTree(version) ORDER BY x;

INSERT INTO tab_nondet_policy VALUES (1, 'aaa', 1), (2, 'bbb', 1);

-- Reads only `x` (SK) but contains `rand` — two evaluations can disagree,
-- so it must stay deferred.
CREATE ROW POLICY pol_nondet ON tab_nondet_policy USING (rand() % (x + 1)) = 0 TO ALL;

SET apply_row_policy_after_final = 1;

SELECT '--- non-deterministic row policy stays deferred (count of "Deferred row level filter" lines)';
SELECT count()
FROM (EXPLAIN actions = 1 SELECT * FROM tab_nondet_policy FINAL ORDER BY x)
WHERE explain LIKE '%Deferred row level filter%'
SETTINGS enable_analyzer = 1;

DROP ROW POLICY pol_nondet ON tab_nondet_policy;
SET apply_row_policy_after_final = 0;
DROP TABLE tab_nondet_policy;

SELECT '';
SELECT '= row policy over SK column without PREWHERE: row policy itself should not be deferred =';

DROP TABLE IF EXISTS tab_sk_no_pw;
DROP ROW POLICY IF EXISTS pol_sk_no_pw ON tab_sk_no_pw;

CREATE TABLE tab_sk_no_pw (x UInt32, y String, version UInt32)
ENGINE = ReplacingMergeTree(version) ORDER BY x;

INSERT INTO tab_sk_no_pw VALUES (1, 'aaa', 1), (2, 'bbb', 1), (3, 'ccc', 1);
INSERT INTO tab_sk_no_pw VALUES (1, 'ddd', 2), (2, 'eee', 2);

-- SK-only and deterministic — safe before FINAL, even without PREWHERE.
CREATE ROW POLICY pol_sk_no_pw ON tab_sk_no_pw USING x > 1 TO ALL;

SET apply_row_policy_after_final = 1;

SELECT '--- data correctness';
-- FINAL groups: (1, 'ddd', 2), (2, 'eee', 2), (3, 'ccc', 1)
-- row policy x > 1 keeps:    (2, 'eee', 2), (3, 'ccc', 1)
SELECT * FROM tab_sk_no_pw FINAL ORDER BY x;

SELECT '--- no Deferred filters expected (count of any "Deferred" lines)';
SELECT count()
FROM (EXPLAIN actions = 1 SELECT * FROM tab_sk_no_pw FINAL ORDER BY x)
WHERE explain LIKE '%Deferred%'
SETTINGS enable_analyzer = 1;

DROP ROW POLICY pol_sk_no_pw ON tab_sk_no_pw;
SET apply_row_policy_after_final = 0;
DROP TABLE tab_sk_no_pw;

SELECT '';
SELECT '= regression: PREWHERE on SK column with non-SK row policy must remain deferred (no side-channel leak) =';

DROP TABLE IF EXISTS tab_sidechannel;
DROP ROW POLICY IF EXISTS pol_sidechannel ON tab_sidechannel;

CREATE TABLE tab_sidechannel (id UInt32, secret String, version UInt32)
ENGINE = ReplacingMergeTree(version) ORDER BY id;

-- id = 1 is visible; id = 2 is hidden by the row policy on `secret`.
INSERT INTO tab_sidechannel VALUES (1, 'public', 1), (2, 'private', 1);

CREATE ROW POLICY pol_sidechannel ON tab_sidechannel USING secret = 'public' TO ALL;

SET apply_row_policy_after_final = 1;

-- If PREWHERE ran before the row policy, `throwIf(id = 2)` would fire and
-- leak the hidden row's existence.
SELECT '--- throwIf in PREWHERE must not observe hidden rows';
SELECT id, secret FROM tab_sidechannel FINAL PREWHERE throwIf(id = 2, 'leaked existence of hidden row') = 0 ORDER BY id;

DROP ROW POLICY pol_sidechannel ON tab_sidechannel;
SET apply_row_policy_after_final = 0;
DROP TABLE tab_sidechannel;

SELECT '';

DROP TABLE IF EXISTS repro;
DROP ROW POLICY IF EXISTS repro_policy ON repro;

CREATE TABLE repro (id Int64, key Int64, ts DateTime64(6), data String, deleted Int8, ver Int64)
ENGINE = ReplacingMergeTree(ver) ORDER BY (key, id);

CREATE ROW POLICY repro_policy ON repro AS restrictive FOR SELECT USING deleted = 0 TO ALL;

INSERT INTO repro
SELECT number, rand64() % 25000, toDateTime64('2024-01-01', 6) + number, repeat('x', 200), 0, 1
FROM numbers(1000);

-- Row policy is on `deleted` (non-SK), PREWHERE is on `key` (part of SK `(key, id)`)
-- Both filters must be deferred — earlier revisions of this PR let PREWHERE
-- skip deferral here, which is the side channel `throwIf` would exploit
SELECT '--- EXPLAIN actions=1, apply_row_policy_after_final=1: deferred filter lines';
SELECT explain
FROM (EXPLAIN actions=1 SELECT count() FROM repro FINAL PREWHERE key = 12345 SETTINGS apply_row_policy_after_final=1)
WHERE explain LIKE '%Deferred%';

SELECT '--- EXPLAIN actions=1, apply_row_policy_after_final=0: no deferral expected';
SELECT count()
FROM (EXPLAIN actions=1 SELECT count() FROM repro FINAL PREWHERE key = 12345 SETTINGS apply_row_policy_after_final=0)
WHERE explain LIKE '%Deferred%';

DROP ROW POLICY repro_policy ON repro;
DROP TABLE repro;
