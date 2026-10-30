-- https://github.com/ClickHouse/ClickHouse/issues/107987
DROP TABLE IF EXISTS table_with_cyclic_defaults_tuple_subcolumns;

CREATE TABLE table_with_cyclic_defaults_tuple_subcolumns
(
    a Tuple(x Int32, y Int32) DEFAULT (b.x, b.x),
    b Tuple(x Int32, y Int32) DEFAULT (c.x, 2),
    c Tuple(x Int32, y Int32) DEFAULT (a.x, 2)
) ENGINE = Memory; -- {serverError CYCLIC_ALIASES}

SELECT 1;
