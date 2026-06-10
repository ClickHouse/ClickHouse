-- An explicitly set `optimize_row_order = 0` opts a table without a sorting key out of
-- row order optimization, even though `optimize_row_order_if_no_order_by` is enabled by default.

SET max_insert_threads = 1;
SET max_threads = 1;

DROP TABLE IF EXISTS tab_optimized;
DROP TABLE IF EXISTS tab_opt_out;
DROP TABLE IF EXISTS tab_opt_out_explicit_on;
DROP TABLE IF EXISTS tab_opt_out_projection;

-- Baseline: without the opt-out, rows of a table with an empty sorting key are reordered on insert.
CREATE TABLE tab_optimized (
    name String,
    timestamp Int64,
    money UInt8,
    flag String
) ENGINE = MergeTree
ORDER BY ()
    -- Disable add_minmax_index_for_numeric_columns since it affects the order
SETTINGS add_minmax_index_for_numeric_columns = 0;

-- Explicit `optimize_row_order = 0`, the new setting omitted (default 1): insertion order must be preserved.
CREATE TABLE tab_opt_out (
    name String,
    timestamp Int64,
    money UInt8,
    flag String
) ENGINE = MergeTree
ORDER BY ()
SETTINGS optimize_row_order = 0, add_minmax_index_for_numeric_columns = 0;

-- The explicit opt-out wins even when `optimize_row_order_if_no_order_by` is explicitly enabled.
CREATE TABLE tab_opt_out_explicit_on (
    name String,
    timestamp Int64,
    money UInt8,
    flag String
) ENGINE = MergeTree
ORDER BY ()
SETTINGS optimize_row_order = 0, optimize_row_order_if_no_order_by = 1, add_minmax_index_for_numeric_columns = 0;

-- The opt-out applies to projection writes as well.
CREATE TABLE tab_opt_out_projection (
    name String,
    timestamp Int64,
    money UInt8,
    flag String,
    PROJECTION p (SELECT name, money ORDER BY name)
) ENGINE = MergeTree
ORDER BY ()
SETTINGS optimize_row_order = 0, add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab_optimized VALUES ('Bob', 4, 100, '1'), ('Nikita', 2, 54, '1'), ('Nikita', 1, 228, '1'), ('Alex', 4, 83, '1'), ('Alex', 4, 134, '1'), ('Alex', 1, 65, '0'), ('Alex', 4, 134, '1'), ('Bob', 2, 53, '0'), ('Alex', 4, 83, '0'), ('Alex', 1, 63, '1'), ('Bob', 2, 53, '1'), ('Alex', 4, 192, '1'), ('Alex', 2, 128, '1'), ('Nikita', 2, 148, '0'), ('Bob', 4, 177, '0'), ('Nikita', 1, 173, '0'), ('Alex', 1, 239, '0'), ('Alex', 1, 63, '0'), ('Alex', 2, 224, '1'), ('Bob', 4, 177, '0'), ('Alex', 2, 128, '1'), ('Alex', 4, 134, '0'), ('Alex', 4, 83, '1'), ('Bob', 4, 100, '0'), ('Nikita', 2, 54, '1'), ('Alex', 1, 239, '1'), ('Bob', 2, 187, '1'), ('Alex', 1, 65, '1'), ('Bob', 2, 53, '1'), ('Alex', 2, 224, '0'), ('Alex', 4, 192, '0'), ('Nikita', 1, 173, '1'), ('Nikita', 2, 148, '1'), ('Bob', 2, 187, '1'), ('Nikita', 2, 208, '1'), ('Nikita', 2, 208, '0'), ('Nikita', 1, 228, '0'), ('Nikita', 2, 148, '0');

INSERT INTO tab_opt_out VALUES ('Bob', 4, 100, '1'), ('Nikita', 2, 54, '1'), ('Nikita', 1, 228, '1'), ('Alex', 4, 83, '1'), ('Alex', 4, 134, '1'), ('Alex', 1, 65, '0'), ('Alex', 4, 134, '1'), ('Bob', 2, 53, '0'), ('Alex', 4, 83, '0'), ('Alex', 1, 63, '1'), ('Bob', 2, 53, '1'), ('Alex', 4, 192, '1'), ('Alex', 2, 128, '1'), ('Nikita', 2, 148, '0'), ('Bob', 4, 177, '0'), ('Nikita', 1, 173, '0'), ('Alex', 1, 239, '0'), ('Alex', 1, 63, '0'), ('Alex', 2, 224, '1'), ('Bob', 4, 177, '0'), ('Alex', 2, 128, '1'), ('Alex', 4, 134, '0'), ('Alex', 4, 83, '1'), ('Bob', 4, 100, '0'), ('Nikita', 2, 54, '1'), ('Alex', 1, 239, '1'), ('Bob', 2, 187, '1'), ('Alex', 1, 65, '1'), ('Bob', 2, 53, '1'), ('Alex', 2, 224, '0'), ('Alex', 4, 192, '0'), ('Nikita', 1, 173, '1'), ('Nikita', 2, 148, '1'), ('Bob', 2, 187, '1'), ('Nikita', 2, 208, '1'), ('Nikita', 2, 208, '0'), ('Nikita', 1, 228, '0'), ('Nikita', 2, 148, '0');

INSERT INTO tab_opt_out_explicit_on VALUES ('Bob', 4, 100, '1'), ('Nikita', 2, 54, '1'), ('Nikita', 1, 228, '1'), ('Alex', 4, 83, '1'), ('Alex', 4, 134, '1'), ('Alex', 1, 65, '0'), ('Alex', 4, 134, '1'), ('Bob', 2, 53, '0'), ('Alex', 4, 83, '0'), ('Alex', 1, 63, '1'), ('Bob', 2, 53, '1'), ('Alex', 4, 192, '1'), ('Alex', 2, 128, '1'), ('Nikita', 2, 148, '0'), ('Bob', 4, 177, '0'), ('Nikita', 1, 173, '0'), ('Alex', 1, 239, '0'), ('Alex', 1, 63, '0'), ('Alex', 2, 224, '1'), ('Bob', 4, 177, '0'), ('Alex', 2, 128, '1'), ('Alex', 4, 134, '0'), ('Alex', 4, 83, '1'), ('Bob', 4, 100, '0'), ('Nikita', 2, 54, '1'), ('Alex', 1, 239, '1'), ('Bob', 2, 187, '1'), ('Alex', 1, 65, '1'), ('Bob', 2, 53, '1'), ('Alex', 2, 224, '0'), ('Alex', 4, 192, '0'), ('Nikita', 1, 173, '1'), ('Nikita', 2, 148, '1'), ('Bob', 2, 187, '1'), ('Nikita', 2, 208, '1'), ('Nikita', 2, 208, '0'), ('Nikita', 1, 228, '0'), ('Nikita', 2, 148, '0');

INSERT INTO tab_opt_out_projection VALUES ('Bob', 4, 100, '1'), ('Nikita', 2, 54, '1'), ('Nikita', 1, 228, '1'), ('Alex', 4, 83, '1'), ('Alex', 4, 134, '1'), ('Alex', 1, 65, '0'), ('Alex', 4, 134, '1'), ('Bob', 2, 53, '0'), ('Alex', 4, 83, '0'), ('Alex', 1, 63, '1'), ('Bob', 2, 53, '1'), ('Alex', 4, 192, '1'), ('Alex', 2, 128, '1'), ('Nikita', 2, 148, '0'), ('Bob', 4, 177, '0'), ('Nikita', 1, 173, '0'), ('Alex', 1, 239, '0'), ('Alex', 1, 63, '0'), ('Alex', 2, 224, '1'), ('Bob', 4, 177, '0'), ('Alex', 2, 128, '1'), ('Alex', 4, 134, '0'), ('Alex', 4, 83, '1'), ('Bob', 4, 100, '0'), ('Nikita', 2, 54, '1'), ('Alex', 1, 239, '1'), ('Bob', 2, 187, '1'), ('Alex', 1, 65, '1'), ('Bob', 2, 53, '1'), ('Alex', 2, 224, '0'), ('Alex', 4, 192, '0'), ('Nikita', 1, 173, '1'), ('Nikita', 2, 148, '1'), ('Bob', 2, 187, '1'), ('Nikita', 2, 208, '1'), ('Nikita', 2, 208, '0'), ('Nikita', 1, 228, '0'), ('Nikita', 2, 148, '0');

-- The opted-out tables must keep the insertion order.
SELECT 'opt_out preserves insertion order';
SELECT
    (SELECT groupArray((name, timestamp, money, flag)) FROM (SELECT * FROM tab_opt_out))
    =
    [('Bob', 4, 100, '1'), ('Nikita', 2, 54, '1'), ('Nikita', 1, 228, '1'), ('Alex', 4, 83, '1'), ('Alex', 4, 134, '1'), ('Alex', 1, 65, '0'), ('Alex', 4, 134, '1'), ('Bob', 2, 53, '0'), ('Alex', 4, 83, '0'), ('Alex', 1, 63, '1'), ('Bob', 2, 53, '1'), ('Alex', 4, 192, '1'), ('Alex', 2, 128, '1'), ('Nikita', 2, 148, '0'), ('Bob', 4, 177, '0'), ('Nikita', 1, 173, '0'), ('Alex', 1, 239, '0'), ('Alex', 1, 63, '0'), ('Alex', 2, 224, '1'), ('Bob', 4, 177, '0'), ('Alex', 2, 128, '1'), ('Alex', 4, 134, '0'), ('Alex', 4, 83, '1'), ('Bob', 4, 100, '0'), ('Nikita', 2, 54, '1'), ('Alex', 1, 239, '1'), ('Bob', 2, 187, '1'), ('Alex', 1, 65, '1'), ('Bob', 2, 53, '1'), ('Alex', 2, 224, '0'), ('Alex', 4, 192, '0'), ('Nikita', 1, 173, '1'), ('Nikita', 2, 148, '1'), ('Bob', 2, 187, '1'), ('Nikita', 2, 208, '1'), ('Nikita', 2, 208, '0'), ('Nikita', 1, 228, '0'), ('Nikita', 2, 148, '0')];

SELECT 'opt_out wins over explicit enable';
SELECT
    (SELECT groupArray((name, timestamp, money, flag)) FROM (SELECT * FROM tab_opt_out_explicit_on))
    =
    (SELECT groupArray((name, timestamp, money, flag)) FROM (SELECT * FROM tab_opt_out));

SELECT 'opt_out with projection preserves insertion order';
SELECT
    (SELECT groupArray((name, timestamp, money, flag)) FROM (SELECT * FROM tab_opt_out_projection))
    =
    (SELECT groupArray((name, timestamp, money, flag)) FROM (SELECT * FROM tab_opt_out));

SELECT 'optimized differs from insertion order';
SELECT
    (SELECT groupArray((name, timestamp, money, flag)) FROM (SELECT * FROM tab_optimized))
    !=
    (SELECT groupArray((name, timestamp, money, flag)) FROM (SELECT * FROM tab_opt_out));

DROP TABLE tab_optimized;
DROP TABLE tab_opt_out;
DROP TABLE tab_opt_out_explicit_on;
DROP TABLE tab_opt_out_projection;
