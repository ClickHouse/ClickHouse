DROP TABLE IF EXISTS decimal;

CREATE TABLE IF NOT EXISTS decimal
(
    a DEC(9, 3),
    b DEC(18, 9),
    c DEC(38, 18)
) ENGINE = Memory;

INSERT INTO decimal (a, b, c) VALUES (42.0, -42.0, 42) (0.42, -0.42, .42) (42.42, -42.42, 42.42);
INSERT INTO decimal (a, b, c) FORMAT JSONEachRow {"a":1.1, "b":-1.1, "c":1.1} {"a":1.0, "b":-1.0, "c":1} {"a":0.1, "b":-0.1, "c":.1};
INSERT INTO decimal (a, b, c) FORMAT CSV 2.0,-2.0,2
;
INSERT INTO decimal (a, b, c) FORMAT CSV 0.2 ,-0.2 ,.2
;
INSERT INTO decimal (a, b, c) FORMAT CSV 2.2 , -2.2 , 2.2
;
INSERT INTO decimal (a, b, c) FORMAT TabSeparated 3.3	-3.3	3.3
;
INSERT INTO decimal (a, b, c) FORMAT TabSeparated 3.0	-3.0	3
;
INSERT INTO decimal (a, b, c) FORMAT TabSeparated 0.3	-0.3	.3
;
INSERT INTO decimal (a, b, c) FORMAT CSV 4.4E+5,-4E+8,.4E+20
;
INSERT INTO decimal (a, b, c) FORMAT CSV 5.5e-2, -5e-9 ,.5e-17
;

SELECT * FROM decimal ORDER BY a FORMAT JSONEachRow;
SELECT * FROM decimal ORDER BY b DESC FORMAT CSV;
SELECT * FROM decimal ORDER BY c FORMAT TabSeparated;

DROP TABLE IF EXISTS decimal;
