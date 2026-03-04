-- Test for issue #75677

DROP TABLE IF EXISTS tab1;

CREATE TABLE tab1 (
    a Int32,
    b String,
    c Float64)
ENGINE MergeTree
ORDER BY a
SETTINGS
    add_minmax_index_for_numeric_columns = 1,
    add_minmax_index_for_string_columns = 1;

CREATE TABLE tab2 AS tab1;

DROP TABLE tab1;
DROP TABLE tab2;
