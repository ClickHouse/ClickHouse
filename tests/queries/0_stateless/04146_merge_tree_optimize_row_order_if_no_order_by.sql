SET max_insert_threads = 1;

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

-- Case 2: setting omitted, should still optimize because the default is 1.
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

-- The optimized order should be identical with the setting omitted (default 1) and explicitly enabled.
SELECT 'default == explicit_on';
SELECT count() = 0 FROM (
    SELECT * FROM tab_explicit_on EXCEPT SELECT * FROM tab_default
    UNION ALL
    SELECT * FROM tab_default EXCEPT SELECT * FROM tab_explicit_on
);
SELECT
    (SELECT groupArray((name, timestamp, money, flag)) FROM (SELECT * FROM tab_explicit_on))
    =
    (SELECT groupArray((name, timestamp, money, flag)) FROM (SELECT * FROM tab_default));

-- With the setting disabled, the on-disk order must be the insertion order, which differs from the optimized order.
SELECT 'disabled != explicit_on';
SELECT
    (SELECT groupArray((name, timestamp, money, flag)) FROM (SELECT * FROM tab_explicit_on))
    !=
    (SELECT groupArray((name, timestamp, money, flag)) FROM (SELECT * FROM tab_disabled));

SELECT 'optimized rows';
SELECT * FROM tab_explicit_on SETTINGS max_threads = 1;

DROP TABLE tab_explicit_on;
DROP TABLE tab_default;
DROP TABLE tab_disabled;
