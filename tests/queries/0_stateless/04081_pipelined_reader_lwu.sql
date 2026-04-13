-- Tags: no-random-merge-tree-settings

SET enable_lightweight_update = 1;
SET enable_multiple_prewhere_read_steps = 1;
SET move_all_conditions_to_prewhere = 1;

DROP TABLE IF EXISTS t_pipelined_lwu;

CREATE TABLE t_pipelined_lwu (key UInt64, pw1 UInt64, pw2 String, rest1 UInt64, rest2 String)
ENGINE = MergeTree ORDER BY key
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1;

INSERT INTO t_pipelined_lwu SELECT number, number % 10, toString(number % 5), number * 100, toString(number) FROM numbers(1000);

-- Case 1: LWU updates rest columns only.
-- Prewhere reads pw1, rest reads patched rest1.
UPDATE t_pipelined_lwu SET rest1 = 999999 WHERE key < 100;

SELECT 'rest_only_pipelined';
SELECT count(), sum(rest1) FROM t_pipelined_lwu PREWHERE pw1 = 0 SETTINGS use_pipelined_mergetree_reader = 1;

SELECT 'rest_only_standard';
SELECT count(), sum(rest1) FROM t_pipelined_lwu PREWHERE pw1 = 0 SETTINGS use_pipelined_mergetree_reader = 0;

-- Case 2: LWU updates prewhere column only.
UPDATE t_pipelined_lwu SET pw2 = 'patched' WHERE key >= 500 AND key < 600;

SELECT 'prewhere_only_pipelined';
SELECT count() FROM t_pipelined_lwu PREWHERE pw2 = 'patched' SETTINGS use_pipelined_mergetree_reader = 1;

SELECT 'prewhere_only_standard';
SELECT count() FROM t_pipelined_lwu PREWHERE pw2 = 'patched' SETTINGS use_pipelined_mergetree_reader = 0;

-- Case 3: LWU updates both prewhere and rest columns.
UPDATE t_pipelined_lwu SET pw1 = 42, rest2 = 'both_patched' WHERE key >= 200 AND key < 300;

SELECT 'both_pipelined';
SELECT count(), min(rest2), max(rest2) FROM t_pipelined_lwu PREWHERE pw1 = 42 SETTINGS use_pipelined_mergetree_reader = 1;

SELECT 'both_standard';
SELECT count(), min(rest2), max(rest2) FROM t_pipelined_lwu PREWHERE pw1 = 42 SETTINGS use_pipelined_mergetree_reader = 0;

-- Case 4: Multi-step prewhere with LWU.
-- pw1 and pw2 are separate prewhere steps; both have patches.
SELECT 'multistep_pipelined';
SELECT count() FROM t_pipelined_lwu PREWHERE pw1 = 42 AND pw2 = 'patched' SETTINGS use_pipelined_mergetree_reader = 1;

SELECT 'multistep_standard';
SELECT count() FROM t_pipelined_lwu PREWHERE pw1 = 42 AND pw2 = 'patched' SETTINGS use_pipelined_mergetree_reader = 0;

-- Case 5: Full row comparison between readers.
SELECT 'full_comparison';
SELECT
    (SELECT groupArray(tuple(key, pw1, pw2, rest1, rest2)) FROM (SELECT * FROM t_pipelined_lwu ORDER BY key SETTINGS use_pipelined_mergetree_reader = 1))
    =
    (SELECT groupArray(tuple(key, pw1, pw2, rest1, rest2)) FROM (SELECT * FROM t_pipelined_lwu ORDER BY key SETTINGS use_pipelined_mergetree_reader = 0))
    AS match;

DROP TABLE t_pipelined_lwu;
