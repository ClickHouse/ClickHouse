DROP TABLE IF EXISTS t_values_default;

CREATE TABLE t_values_default
(
    a UInt64,
    b UInt64 DEFAULT 42,
    c UInt64 DEFAULT a * a,
    d Bool
) ENGINE = Memory;

INSERT INTO t_values_default VALUES (1 + 1, DEFAULT, 3, true), (1 + 2, 2, DEFAULT, 1), (4, DEFAULT, DEFAULT, DEFAULT);
INSERT INTO t_values_default VALUES (5, 6, 7, true), ('6', '7', '8', true), (DEFAULT, DEFAULT, DEFAULT, DEFAULT);

SET input_format_values_interpret_expressions=0;
SET input_format_values_deduce_templates_of_expressions=0;

INSERT INTO t_values_default VALUES (5, 6, 7, true), ('6', '7', '8', true), (DEFAULT, DEFAULT, DEFAULT, DEFAULT); -- {clientError 344}
INSERT INTO t_values_default VALUES (7, 8, 9, true), (8, DEFAULT, DEFAULT, DEFAULT);

SELECT * FROM t_values_default ORDER BY a;

DROP TABLE t_values_default;
