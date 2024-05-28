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
SETTINGS allow_experimental_optimized_row_order = True;

INSERT INTO tab VALUES ('A', '2020-01-01', [0.0, 1.1], 10, 'some string', {'key':'value'}, (123)), ('A', '2020-01-01', [0.0, 1.1], NULL, 'example', {}, (26)), ('A', '2020-01-01', [2.2, 1.1], 1, 'some other string', {'key2':'value2'}, (5)), ('A', '2020-01-02', [0.0, 1.1], 10, 'some string', {'key':'value'}, (123)), ('A', '2020-01-02', [0.0, 2.2], 10, 'example', {}, (26)), ('A', '2020-01-02', [2.2, 1.1], 1, 'some other string', {'key2':'value2'}, (5)), ('B', '2020-01-04', [0.0, 1.1], 10, 'some string', {'key':'value'}, (123)), ('B', '2020-01-04', [0.0, 2.2], Null, 'example', {}, (26)), ('B', '2020-01-04', [2.2, 1.1], 1, 'some string', {'key2':'value2'}, (5)), ('B', '2020-01-05', [0.0, 1.1], 10, 'some string', {'key':'value'}, (123)), ('B', '2020-01-05', [0.0, 2.2], Null, 'example', {}, (26)), ('B', '2020-01-05', [2.2, 1.1], 1, 'some other string', {'key':'value'}, (5)), ('C', '2020-01-04', [0.0, 1.1], 10, 'some string', {'key':'value'}, (5)), ('C', '2020-01-04', [0.0, 2.2], Null, 'example', {}, (26)), ('C', '2020-01-04', [2.2, 1.1], 1, 'some other string', {'key2':'value2'}, (5));

SELECT * FROM tab SETTINGS max_threads=1;

DROP TABLE tab;
