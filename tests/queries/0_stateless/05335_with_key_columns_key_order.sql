-- Tags: no-random-merge-tree-settings
SET allow_experimental_map_key_columns = 1;
-- Tag no-random-merge-tree-settings: the randomized `map_serialization_version` replaces `with_key_columns`

-- A full map is read in key order whatever order the keys were inserted in, before and after merges,
-- and for a key that first appears after some granules were written.

DROP TABLE IF EXISTS t_wkc_key_order;

CREATE TABLE t_wkc_key_order (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    index_granularity = 1000,
    index_granularity_bytes = '10Mi';

SYSTEM STOP MERGES t_wkc_key_order;

INSERT INTO t_wkc_key_order VALUES (1, map('c', 3, 'b', 2, 'a', 1));
SELECT 'insert', m, mapKeys(m), mapValues(m) FROM t_wkc_key_order ORDER BY id;

INSERT INTO t_wkc_key_order VALUES (2, map('d', 4, 'aa', 5));
SELECT 'two parts', id, m FROM t_wkc_key_order ORDER BY id;

SYSTEM START MERGES t_wkc_key_order;
OPTIMIZE TABLE t_wkc_key_order FINAL;
SELECT 'merged', id, m FROM t_wkc_key_order ORDER BY id;

INSERT INTO t_wkc_key_order SELECT number + 10, map('z', number) FROM numbers(20000);
OPTIMIZE TABLE t_wkc_key_order FINAL;
ALTER TABLE t_wkc_key_order UPDATE m = map('z', id - 10, '0', id) WHERE id >= 15000 SETTINGS mutations_sync = 2;
SELECT 'late key', id, m FROM t_wkc_key_order WHERE id IN (1, 2, 14999, 15000) ORDER BY id;

DROP TABLE t_wkc_key_order;
