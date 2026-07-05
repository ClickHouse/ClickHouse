-- Tests that the row order optimization (see optimize_row_order) is applied automatically
-- for ordinary MergeTree tables that have no ORDER BY key, governed by the new setting
-- optimize_row_order_if_no_order_by (default: 1). See issue #103839.

-- One insert thread -> one part, so SELECT without ORDER BY returns the physical row order.
SET max_insert_threads = 1;
SET max_threads = 1;

-- The new setting exists and defaults to 1.
SELECT 'default value:', value FROM system.merge_tree_settings WHERE name = 'optimize_row_order_if_no_order_by';

DROP TABLE IF EXISTS tab_auto;
DROP TABLE IF EXISTS tab_explicit;
DROP TABLE IF EXISTS tab_disabled;

-- Column `a` has cardinality 2 and `b` cardinality 1, so the optimizer groups rows by `a`,
-- producing a physical order that differs from the insertion order (1,2,3,4,5).
-- add_minmax_index_for_numeric_columns is disabled because it can influence the chosen order.

-- (1) No ORDER BY key, all settings default -> relies on optimize_row_order_if_no_order_by = 1.
CREATE TABLE tab_auto (id UInt32, a UInt8, b UInt8) ENGINE = MergeTree ORDER BY ()
SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO tab_auto VALUES (1, 1, 100), (2, 2, 100), (3, 1, 100), (4, 2, 100), (5, 1, 100);

-- (2) No ORDER BY key, optimize_row_order explicitly enabled -> reference "optimized" order.
CREATE TABLE tab_explicit (id UInt32, a UInt8, b UInt8) ENGINE = MergeTree ORDER BY ()
SETTINGS optimize_row_order = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO tab_explicit VALUES (1, 1, 100), (2, 2, 100), (3, 1, 100), (4, 2, 100), (5, 1, 100);

-- (3) No ORDER BY key, optimization disabled via the new setting -> keeps insertion order.
CREATE TABLE tab_disabled (id UInt32, a UInt8, b UInt8) ENGINE = MergeTree ORDER BY ()
SETTINGS optimize_row_order_if_no_order_by = 0, add_minmax_index_for_numeric_columns = 0;
INSERT INTO tab_disabled VALUES (1, 1, 100), (2, 2, 100), (3, 1, 100), (4, 2, 100), (5, 1, 100);

-- The automatic default path must produce exactly the same physical order as explicitly
-- enabling optimize_row_order (both run the identical RowOrderOptimizer with an empty sort key).
SELECT 'auto == explicit:',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_auto))
  = (SELECT groupArray(id) FROM (SELECT id FROM tab_explicit));

-- The automatic path must reorder rows, i.e. differ from the raw insertion order.
SELECT 'auto reorders (!= insertion order):',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_auto)) != [1, 2, 3, 4, 5];

-- Disabling the setting must keep the raw insertion order.
SELECT 'disabled keeps insertion order:',
    (SELECT groupArray(id) FROM (SELECT id FROM tab_disabled)) = [1, 2, 3, 4, 5];

DROP TABLE tab_auto;
DROP TABLE tab_explicit;
DROP TABLE tab_disabled;
