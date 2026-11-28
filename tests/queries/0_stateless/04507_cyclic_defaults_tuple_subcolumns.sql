-- https://github.com/ClickHouse/ClickHouse/issues/107987
DROP TABLE IF EXISTS table_with_cyclic_defaults_tuple_subcolumns;

CREATE TABLE table_with_cyclic_defaults_tuple_subcolumns
(
    a Tuple(x Int32, y Int32) DEFAULT (b.x, b.x),
    b Tuple(x Int32, y Int32) DEFAULT (c.x, 2),
    c Tuple(x Int32, y Int32) DEFAULT (a.x, 2)
) ENGINE = Memory; -- {serverError CYCLIC_ALIASES}

-- The same cycle with each subcolumn written as one quoted identifier.
CREATE TABLE table_with_cyclic_defaults_tuple_subcolumns
(
    a Tuple(x Int32, y Int32) DEFAULT (`b.x`, `b.x`),
    b Tuple(x Int32, y Int32) DEFAULT (`c.x`, 2),
    c Tuple(x Int32, y Int32) DEFAULT (`a.x`, 2)
) ENGINE = Memory; -- {serverError CYCLIC_ALIASES}

-- Without a cycle, both spellings resolve to the subcolumn.
CREATE TABLE table_with_cyclic_defaults_tuple_subcolumns
(
    a Tuple(x Int32, y Int32),
    b Int32 DEFAULT a.x,
    c Int32 DEFAULT `a.y`
) ENGINE = Memory;

INSERT INTO table_with_cyclic_defaults_tuple_subcolumns (a) VALUES ((1, 2));
SELECT * FROM table_with_cyclic_defaults_tuple_subcolumns;

DROP TABLE table_with_cyclic_defaults_tuple_subcolumns;
