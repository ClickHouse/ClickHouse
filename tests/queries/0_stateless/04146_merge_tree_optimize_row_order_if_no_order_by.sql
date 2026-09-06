SET max_insert_threads = 1;
SET max_threads = 1;

DROP TABLE IF EXISTS tab_explicit_on;
DROP TABLE IF EXISTS tab_default;
DROP TABLE IF EXISTS tab_disabled;

-- Case 1: setting explicitly enabled.
CREATE TABLE tab_explicit_on (
    name String,
    timestamp Int64,
    money UInt8,
    flag String
) ENGINE = MergeTree
ORDER BY ()
    -- Disable add_minmax_index_for_numeric_columns since it affects the order
SETTINGS optimize_row_order_if_no_order_by = True, add_minmax_index_for_numeric_columns = 0;

-- Case 2: setting omitted, should preserve insertion order because the default is 0.
CREATE TABLE tab_default (
    name String,
    timestamp Int64,
    money UInt8,
    flag String
) ENGINE = MergeTree
ORDER BY ()
SETTINGS add_minmax_index_for_numeric_columns = 0;

-- Case 3: setting disabled, should preserve insertion order.
CREATE TABLE tab_disabled (
    name String,
    timestamp Int64,
    money UInt8,
    flag String
) ENGINE = MergeTree
ORDER BY ()
SETTINGS optimize_row_order_if_no_order_by = False, add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab_explicit_on VALUES ('Bob', 4, 100, '1'), ('Nikita', 2, 54, '1'), ('Nikita', 1, 228, '1'), ('Alex', 4, 83, '1'), ('Alex', 4, 134, '1'), ('Alex', 1, 65, '0'), ('Alex', 4, 134, '1'), ('Bob', 2, 53, '0'), ('Alex', 4, 83, '0'), ('Alex', 1, 63, '1'), ('Bob', 2, 53, '1'), ('Alex', 4, 192, '1'), ('Alex', 2, 128, '1'), ('Nikita', 2, 148, '0'), ('Bob', 4, 177, '0'), ('Nikita', 1, 173, '0'), ('Alex', 1, 239, '0'), ('Alex', 1, 63, '0'), ('Alex', 2, 224, '1'), ('Bob', 4, 177, '0'), ('Alex', 2, 128, '1'), ('Alex', 4, 134, '0'), ('Alex', 4, 83, '1'), ('Bob', 4, 100, '0'), ('Nikita', 2, 54, '1'), ('Alex', 1, 239, '1'), ('Bob', 2, 187, '1'), ('Alex', 1, 65, '1'), ('Bob', 2, 53, '1'), ('Alex', 2, 224, '0'), ('Alex', 4, 192, '0'), ('Nikita', 1, 173, '1'), ('Nikita', 2, 148, '1'), ('Bob', 2, 187, '1'), ('Nikita', 2, 208, '1'), ('Nikita', 2, 208, '0'), ('Nikita', 1, 228, '0'), ('Nikita', 2, 148, '0');

INSERT INTO tab_default VALUES ('Bob', 4, 100, '1'), ('Nikita', 2, 54, '1'), ('Nikita', 1, 228, '1'), ('Alex', 4, 83, '1'), ('Alex', 4, 134, '1'), ('Alex', 1, 65, '0'), ('Alex', 4, 134, '1'), ('Bob', 2, 53, '0'), ('Alex', 4, 83, '0'), ('Alex', 1, 63, '1'), ('Bob', 2, 53, '1'), ('Alex', 4, 192, '1'), ('Alex', 2, 128, '1'), ('Nikita', 2, 148, '0'), ('Bob', 4, 177, '0'), ('Nikita', 1, 173, '0'), ('Alex', 1, 239, '0'), ('Alex', 1, 63, '0'), ('Alex', 2, 224, '1'), ('Bob', 4, 177, '0'), ('Alex', 2, 128, '1'), ('Alex', 4, 134, '0'), ('Alex', 4, 83, '1'), ('Bob', 4, 100, '0'), ('Nikita', 2, 54, '1'), ('Alex', 1, 239, '1'), ('Bob', 2, 187, '1'), ('Alex', 1, 65, '1'), ('Bob', 2, 53, '1'), ('Alex', 2, 224, '0'), ('Alex', 4, 192, '0'), ('Nikita', 1, 173, '1'), ('Nikita', 2, 148, '1'), ('Bob', 2, 187, '1'), ('Nikita', 2, 208, '1'), ('Nikita', 2, 208, '0'), ('Nikita', 1, 228, '0'), ('Nikita', 2, 148, '0');

INSERT INTO tab_disabled VALUES ('Bob', 4, 100, '1'), ('Nikita', 2, 54, '1'), ('Nikita', 1, 228, '1'), ('Alex', 4, 83, '1'), ('Alex', 4, 134, '1'), ('Alex', 1, 65, '0'), ('Alex', 4, 134, '1'), ('Bob', 2, 53, '0'), ('Alex', 4, 83, '0'), ('Alex', 1, 63, '1'), ('Bob', 2, 53, '1'), ('Alex', 4, 192, '1'), ('Alex', 2, 128, '1'), ('Nikita', 2, 148, '0'), ('Bob', 4, 177, '0'), ('Nikita', 1, 173, '0'), ('Alex', 1, 239, '0'), ('Alex', 1, 63, '0'), ('Alex', 2, 224, '1'), ('Bob', 4, 177, '0'), ('Alex', 2, 128, '1'), ('Alex', 4, 134, '0'), ('Alex', 4, 83, '1'), ('Bob', 4, 100, '0'), ('Nikita', 2, 54, '1'), ('Alex', 1, 239, '1'), ('Bob', 2, 187, '1'), ('Alex', 1, 65, '1'), ('Bob', 2, 53, '1'), ('Alex', 2, 224, '0'), ('Alex', 4, 192, '0'), ('Nikita', 1, 173, '1'), ('Nikita', 2, 148, '1'), ('Bob', 2, 187, '1'), ('Nikita', 2, 208, '1'), ('Nikita', 2, 208, '0'), ('Nikita', 1, 228, '0'), ('Nikita', 2, 148, '0');

-- The default must preserve insertion order, unlike an explicitly enabled setting.
SELECT 'default != explicit_on';
SELECT
    (SELECT groupArray((name, timestamp, money, flag)) FROM (SELECT * FROM tab_explicit_on))
    !=
    (SELECT groupArray((name, timestamp, money, flag)) FROM (SELECT * FROM tab_default));

-- Explicitly disabling the setting has the same effect as its default.
SELECT 'default == disabled';
SELECT
    (SELECT groupArray((name, timestamp, money, flag)) FROM (SELECT * FROM tab_default))
    =
    (SELECT groupArray((name, timestamp, money, flag)) FROM (SELECT * FROM tab_disabled));

SELECT 'optimized rows';
SELECT * FROM tab_explicit_on SETTINGS max_threads = 1;

DROP TABLE tab_explicit_on;
DROP TABLE tab_default;
DROP TABLE tab_disabled;
