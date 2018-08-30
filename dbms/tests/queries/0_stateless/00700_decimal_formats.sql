SET allow_experimental_decimal_type = 1;
CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.decimal;

CREATE TABLE IF NOT EXISTS test.decimal
(
    a DEC(9, 3),
    b DEC(18, 9),
    c DEC(38, 18)
) ENGINE = Memory;

INSERT INTO test.decimal (a, b, c) VALUES (42.0, -42.0, 42) (0.42, -0.42, .42) (42.42, -42.42, 42.42);
INSERT INTO test.decimal (a, b, c) FORMAT JSONEachRow {"a":1.1, "b":-1.1, "c":1.1} {"a":1.0, "b":-1.0, "c":1} {"a":0.1, "b":-0.1, "c":.1};
INSERT INTO test.decimal (a, b, c) FORMAT CSV 2.0, -2.0, 2
;
INSERT INTO test.decimal (a, b, c) FORMAT CSV 0.2, -0.2, .2
;
INSERT INTO test.decimal (a, b, c) FORMAT CSV 2.2 , -2.2 , 2.2
;
INSERT INTO test.decimal (a, b, c) FORMAT TabSeparated 3.3	-3.3	3.3
;
INSERT INTO test.decimal (a, b, c) FORMAT TabSeparated 3.0	-3.0	3
;
INSERT INTO test.decimal (a, b, c) FORMAT TabSeparated 0.3	-0.3	.3
;

SELECT * FROM test.decimal ORDER BY a FORMAT JSONEachRow;
SELECT * FROM test.decimal ORDER BY b DESC FORMAT CSV;
SELECT * FROM test.decimal ORDER BY c FORMAT TabSeparated;

DROP TABLE IF EXISTS test.decimal;
