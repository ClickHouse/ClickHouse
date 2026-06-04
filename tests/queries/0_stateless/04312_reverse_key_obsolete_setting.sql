-- Descending sort order in MergeTree sorting keys used to be gated by the experimental
-- setting `allow_experimental_reverse_key`. The feature is now always supported and the
-- setting is obsolete. This test checks that:
--   1. a descending sort key works without any setting,
--   2. the obsolete setting is still accepted (does nothing) for backward compatibility,
--   3. the setting is reported as obsolete in `system.merge_tree_settings`.

DROP TABLE IF EXISTS t_reverse_key;

-- A descending sort key is now always allowed, no setting required.
CREATE TABLE t_reverse_key (a Int32, b Int32) ENGINE = MergeTree ORDER BY (a DESC, b);
INSERT INTO t_reverse_key VALUES (1, 10), (3, 30), (2, 20);
SELECT a, b FROM t_reverse_key ORDER BY a DESC, b;

DROP TABLE t_reverse_key;

-- The obsolete setting is still accepted (does nothing) for backward compatibility.
CREATE TABLE t_reverse_key (a Int32, b Int32) ENGINE = MergeTree ORDER BY (a DESC, b)
SETTINGS allow_experimental_reverse_key = 1;
INSERT INTO t_reverse_key VALUES (1, 10), (3, 30), (2, 20);
SELECT a, b FROM t_reverse_key ORDER BY a DESC, b;

DROP TABLE t_reverse_key;

-- The setting is reported as obsolete.
SELECT name, tier, is_obsolete FROM system.merge_tree_settings WHERE name = 'allow_experimental_reverse_key';
