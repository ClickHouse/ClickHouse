-- Same cycle as 04507, with the subcolumn paths written as single quoted identifiers.
DROP TABLE IF EXISTS table_with_cyclic_defaults_quoted_subcolumns;

CREATE TABLE table_with_cyclic_defaults_quoted_subcolumns
(
    a Tuple(x Int32, y Int32) DEFAULT (`b.x`, 1),
    b Tuple(x Int32, y Int32) DEFAULT (`a.x`, 2)
) ENGINE = Memory; -- {serverError CYCLIC_ALIASES}

SELECT 1;
