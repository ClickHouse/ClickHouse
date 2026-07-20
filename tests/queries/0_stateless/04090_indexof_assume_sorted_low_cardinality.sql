-- Tags: no-random-merge-tree-settings

-- Test for issue #81640: indexOfAssumeSorted returns wrong results for Array(LowCardinality(String))
-- The bug was that the LowCardinality optimization compared dictionary indices (assigned in insertion
-- order) instead of actual values during binary search, producing incorrect results.

DROP TABLE IF EXISTS test_index_of_assume_sorted_lc;

CREATE TABLE test_index_of_assume_sorted_lc
(
    id Int64,
    elements Array(String),
    sorted_elements_lc Array(LowCardinality(String)) MATERIALIZED arraySort(elements),
    sorted_elements Array(String) MATERIALIZED arraySort(elements)
) ENGINE = MergeTree()
    PARTITION BY id % 8
    ORDER BY id;

INSERT INTO test_index_of_assume_sorted_lc(id, elements) VALUES
    (1, ['AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'FFF', 'GGG', 'HHH', 'III']),
    (2, ['AAA', 'BBB', 'CCC', 'DDD', 'GGG', 'HHH', 'JJJ', 'KKK', 'LLL', 'MMM']),
    (3, ['AAA', 'BBB', 'CCC', 'DDD', 'HHH', 'KKK', 'LLL', 'MMM', 'NNN']),
    (4, ['AAA', 'CCC', 'DDD', 'EEE', 'HHH', 'NNN', 'OOO', 'PPP', 'QQQ', 'RRR', 'SSS']),
    (5, ['AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'HHH', 'III', 'TTT']),
    (6, ['AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'FFF', 'HHH', 'UUU', 'VVV', 'WWW']),
    (7, ['AAA', 'BBB', 'CCC', 'DDD', 'HHH', 'MMM', 'XXX', 'YYY', 'ZZZ', 'aaa', 'bbb']),
    (8, ['AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'HHH', 'III', 'NNN']),
    (9, ['AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'HHH', 'VVV', 'WWW', 'XXX', 'ccc', 'ddd']),
    (10, ['AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'HHH', 'VVV', 'WWW', 'XXX', 'ccc', 'ddd']),
    (11, ['AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'HHH', 'VVV', 'WWW', 'XXX', 'ccc', 'ddd']),
    (12, ['AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'HHH', 'VVV', 'WWW', 'ccc', 'ddd']),
    (13, ['AAA', 'BBB', 'CCC', 'DDD', 'HHH', 'KKK', 'LLL', 'NNN', 'WWW']),
    (14, ['AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'HHH', 'VVV', 'WWW', 'XXX', 'ccc', 'ddd']),
    (15, ['AAA', 'BBB', 'CCC', 'DDD', 'HHH', 'VVV', 'WWW', 'XXX', 'ccc', 'ddd']),
    (16, ['AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'HHH', 'VVV', 'WWW', 'XXX', 'ccc', 'ddd']),
    (17, ['AAA', 'CCC', 'DDD', 'EEE', 'HHH', 'NNN', 'QQQ', 'eee', 'fff']),
    (18, ['AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'HHH', 'MMM', 'bbb', 'eee', 'ggg', 'hhh']),
    (19, ['AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'HHH', 'VVV', 'WWW', 'ccc', 'ddd']),
    (20, ['AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'HHH', 'NNN', 'WWW']);

-- All 20 rows contain 'HHH', so the count must be 20 for both LC and non-LC columns.
SELECT 'LC String count';
SELECT count() FROM test_index_of_assume_sorted_lc WHERE indexOfAssumeSorted(sorted_elements_lc, 'HHH') > 0;

SELECT 'Non-LC String count';
SELECT count() FROM test_index_of_assume_sorted_lc WHERE indexOfAssumeSorted(sorted_elements, 'HHH') > 0;

-- Verify that results match between LC and non-LC for every row.
SELECT 'Mismatched rows';
SELECT count() FROM test_index_of_assume_sorted_lc
WHERE indexOfAssumeSorted(sorted_elements_lc, 'HHH') != indexOfAssumeSorted(sorted_elements, 'HHH');

-- Also test with LowCardinality(UInt32) to cover numeric types.
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS test_index_of_assume_sorted_lc_uint;

CREATE TABLE test_index_of_assume_sorted_lc_uint
(
    id Int64,
    elements Array(UInt32),
    sorted_lc Array(LowCardinality(UInt32)) MATERIALIZED arraySort(elements),
    sorted_plain Array(UInt32) MATERIALIZED arraySort(elements)
) ENGINE = MergeTree()
    PARTITION BY id % 4
    ORDER BY id;

INSERT INTO test_index_of_assume_sorted_lc_uint(id, elements) VALUES
    (1, [10, 20, 30, 40, 50]),
    (2, [5, 15, 25, 35, 45, 55]),
    (3, [10, 30, 50, 70, 90]),
    (4, [20, 40, 60, 80, 100]),
    (5, [1, 2, 3, 30, 99]),
    (6, [30, 60, 90, 120, 150]);

SELECT 'LC UInt32 count for 30';
SELECT count() FROM test_index_of_assume_sorted_lc_uint WHERE indexOfAssumeSorted(sorted_lc, toUInt32(30)) > 0;

SELECT 'Non-LC UInt32 count for 30';
SELECT count() FROM test_index_of_assume_sorted_lc_uint WHERE indexOfAssumeSorted(sorted_plain, toUInt32(30)) > 0;

SELECT 'UInt32 mismatched rows';
SELECT count() FROM test_index_of_assume_sorted_lc_uint
WHERE indexOfAssumeSorted(sorted_lc, toUInt32(30)) != indexOfAssumeSorted(sorted_plain, toUInt32(30));

DROP TABLE test_index_of_assume_sorted_lc;
DROP TABLE test_index_of_assume_sorted_lc_uint;
