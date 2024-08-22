-- Checks that no bad things happen when the table optimizes the row order to improve compressability during insert.


-- Below SELECTs intentionally only ORDER BY the table primary key and rely on read-in-order optimization
SET optimize_read_in_order = 1;

-- Just simple check, that optimization works correctly for table with 2 columns and 2 equivalence classes.
SELECT 'Simple test';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
    name String,
    event Int8
) ENGINE = MergeTree
ORDER BY name
SETTINGS optimize_row_order = true;
INSERT INTO tab VALUES ('Igor', 3), ('Egor', 1), ('Egor', 2), ('Igor', 2), ('Igor', 1);

SELECT * FROM tab ORDER BY name SETTINGS max_threads=1;

DROP TABLE tab;

-- Checks that RowOptimizer correctly selects the order for columns according to cardinality, with an empty ORDER BY.
-- There are 4 columns with cardinalities {name : 3, timestamp": 3, money: 17, flag: 2}, so the columns order must be {flag, name, timestamp, money}.
SELECT 'Cardinalities test';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
    name String,
    timestamp Int64,
    money UInt8,
    flag String
) ENGINE = MergeTree
ORDER BY ()
SETTINGS optimize_row_order = True;
INSERT INTO tab VALUES ('Bob', 4, 100, '1'), ('Nikita', 2, 54, '1'), ('Nikita', 1, 228, '1'), ('Alex', 4, 83, '1'), ('Alex', 4, 134, '1'), ('Alex', 1, 65, '0'), ('Alex', 4, 134, '1'), ('Bob', 2, 53, '0'), ('Alex', 4, 83, '0'), ('Alex', 1, 63, '1'), ('Bob', 2, 53, '1'), ('Alex', 4, 192, '1'), ('Alex', 2, 128, '1'), ('Nikita', 2, 148, '0'), ('Bob', 4, 177, '0'), ('Nikita', 1, 173, '0'), ('Alex', 1, 239, '0'), ('Alex', 1, 63, '0'), ('Alex', 2, 224, '1'), ('Bob', 4, 177, '0'), ('Alex', 2, 128, '1'), ('Alex', 4, 134, '0'), ('Alex', 4, 83, '1'), ('Bob', 4, 100, '0'), ('Nikita', 2, 54, '1'), ('Alex', 1, 239, '1'), ('Bob', 2, 187, '1'), ('Alex', 1, 65, '1'), ('Bob', 2, 53, '1'), ('Alex', 2, 224, '0'), ('Alex', 4, 192, '0'), ('Nikita', 1, 173, '1'), ('Nikita', 2, 148, '1'), ('Bob', 2, 187, '1'), ('Nikita', 2, 208, '1'), ('Nikita', 2, 208, '0'), ('Nikita', 1, 228, '0'), ('Nikita', 2, 148, '0');

SELECT * FROM tab SETTINGS max_threads=1;

DROP TABLE tab;

-- Checks that RowOptimizer correctly selects the order for columns according to cardinality in each equivalence class obtained using SortDescription.
-- There are two columns in the SortDescription: {flag, money} in this order.
-- So there are 5 equivalence classes: {9.81, 9}, {2.7, 1}, {42, 1}, {3.14, Null}, {42, Null}.
-- For the first three of them cardinalities of the other 2 columns are equal, so they are sorted in order {0, 1} in these classes.
-- In the fourth class cardinalities: {name : 2, timestamp : 3}, so they are sorted in order {name, timestamp} in this class.
-- In the fifth class cardinalities: {name : 3, timestamp : 2}, so they are sorted in order {timestamp, name} in this class.
SELECT 'Equivalence classes test';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
    name FixedString(2),
    timestamp Float32,
    money Float64,
    flag Nullable(Int32)
) ENGINE = MergeTree
ORDER BY (flag, money)
SETTINGS optimize_row_order = True, allow_nullable_key = True;
INSERT INTO tab VALUES ('AB', 0, 42, Null), ('AB', 0, 42, Null), ('A', 1, 42, Null), ('AB', 1, 9.81, 0), ('B', 0, 42, Null), ('B', -1, 3.14, Null), ('B', 1, 2.7, 1), ('B', 0, 42, 1), ('A', 1, 42, 1), ('B', 1, 42, Null), ('B', 0, 2.7, 1), ('A', 0, 2.7, 1), ('B', 2, 3.14, Null), ('A', 0, 3.14, Null), ('A', 1, 2.7, 1), ('A', 1, 42, Null);

SELECT * FROM tab ORDER BY (flag, money) SETTINGS max_threads=1;

DROP TABLE tab;

-- Checks that no bad things happen when the table optimizes the row order to improve compressability during insert for many different column types.
-- For some of these types estimateCardinalityInPermutedRange returns just the size of the current equal range.
-- There are 5 equivalence classes, each of them has equal size = 3.
-- In the first of them cardinality of the vector_array column equals 2, other cardinalities equals 3.
-- In the second of them cardinality of the nullable_int column equals 2, other cardinalities equals 3.
-- ...
-- In the fifth of them cardinality of the tuple_column column equals 2, other cardinalities equals 3.
-- So, for all of this classes for columns with cardinality equals 2 such that estimateCardinalityInPermutedRange methid is implemented, 
-- this column must be the first in the column order, all others must be in the stable order.
-- For all other classes columns must be in the stable order.
SELECT 'Many types test';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
    fixed_str FixedString(6),
    event_date Date,
    vector_array Array(Float32),
    nullable_int Nullable(Int128),
    low_card_string LowCardinality(String),
    map_column Map(String, String),
    tuple_column Tuple(UInt256)
) ENGINE = MergeTree()
ORDER BY (fixed_str, event_date)
SETTINGS optimize_row_order = True;

INSERT INTO tab VALUES ('A', '2020-01-01', [0.0, 1.1], 10, 'some string', {'key':'value'}, (123)), ('A', '2020-01-01', [0.0, 1.1], NULL, 'example', {}, (26)), ('A', '2020-01-01', [2.2, 1.1], 1, 'some other string', {'key2':'value2'}, (5)), ('A', '2020-01-02', [0.0, 1.1], 10, 'some string', {'key':'value'}, (123)), ('A', '2020-01-02', [0.0, 2.2], 10, 'example', {}, (26)), ('A', '2020-01-02', [2.2, 1.1], 1, 'some other string', {'key2':'value2'}, (5)), ('B', '2020-01-04', [0.0, 1.1], 10, 'some string', {'key':'value'}, (123)), ('B', '2020-01-04', [0.0, 2.2], Null, 'example', {}, (26)), ('B', '2020-01-04', [2.2, 1.1], 1, 'some string', {'key2':'value2'}, (5)), ('B', '2020-01-05', [0.0, 1.1], 10, 'some string', {'key':'value'}, (123)), ('B', '2020-01-05', [0.0, 2.2], Null, 'example', {}, (26)), ('B', '2020-01-05', [2.2, 1.1], 1, 'some other string', {'key':'value'}, (5)), ('C', '2020-01-04', [0.0, 1.1], 10, 'some string', {'key':'value'}, (5)), ('C', '2020-01-04', [0.0, 2.2], Null, 'example', {}, (26)), ('C', '2020-01-04', [2.2, 1.1], 1, 'some other string', {'key2':'value2'}, (5));

SELECT * FROM tab ORDER BY (fixed_str, event_date) SETTINGS max_threads=1;

DROP TABLE tab;
