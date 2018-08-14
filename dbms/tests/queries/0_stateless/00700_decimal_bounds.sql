SET allow_experimental_decimal_type=1;
CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.decimal;

CREATE TABLE IF NOT EXISTS test.decimal (x DECIMAL(10, -2)) ENGINE = Memory; -- { serverError 69 }
CREATE TABLE IF NOT EXISTS test.decimal (x DECIMAL(10, 15)) ENGINE = Memory; -- { serverError 69 }
CREATE TABLE IF NOT EXISTS test.decimal (x DECIMAL(0, 0)) ENGINE = Memory; -- { serverError 69 }

CREATE TABLE IF NOT EXISTS test.decimal
(
    a DECIMAL(9,0),
    b DECIMAL(18,0),
    c DECIMAL(38,0),
    d DECIMAL(9, 9),
    e DECIMAL(18, 18),
    f DECIMAL(38, 38),
    g Decimal(9, 5),
    h decimal(18, 9),
    i deciMAL(38, 18),
    j DECIMAL(1,0)
) ENGINE = Memory;

INSERT INTO test.decimal (a) VALUES (1000000000); -- { clientError 69 }
INSERT INTO test.decimal (a) VALUES (-1000000000); -- { clientError 69 }
INSERT INTO test.decimal (b) VALUES (1000000000000000000); -- { clientError 69 }
INSERT INTO test.decimal (b) VALUES (-1000000000000000000); -- { clientError 69 }
INSERT INTO test.decimal (c) VALUES (100000000000000000000000000000000000000); -- { clientError 69 }
INSERT INTO test.decimal (c) VALUES (-100000000000000000000000000000000000000); -- { clientError 69 }
INSERT INTO test.decimal (d) VALUES (1); -- { clientError 69 }
INSERT INTO test.decimal (d) VALUES (-1); -- { clientError 69 }
INSERT INTO test.decimal (e) VALUES (1000000000000000000); -- { clientError 69 }
INSERT INTO test.decimal (e) VALUES (-1000000000000000000); -- { clientError 69 }
INSERT INTO test.decimal (f) VALUES (1); -- { clientError 69 }
INSERT INTO test.decimal (f) VALUES (-1); -- { clientError 69 }
INSERT INTO test.decimal (g) VALUES (10000); -- { clientError 69 }
INSERT INTO test.decimal (g) VALUES (-10000); -- { clientError 69 }
INSERT INTO test.decimal (h) VALUES (1000000000); -- { clientError 69 }
INSERT INTO test.decimal (h) VALUES (-1000000000); -- { clientError 69 }
INSERT INTO test.decimal (i) VALUES (100000000000000000000); -- { clientError 69 }
INSERT INTO test.decimal (i) VALUES (-100000000000000000000); -- { clientError 69 }
INSERT INTO test.decimal (j) VALUES (10); -- { clientError 69 }
INSERT INTO test.decimal (j) VALUES (-10); -- { clientError 69 }

INSERT INTO test.decimal (a) VALUES (0.1); -- { clientError 69 }
INSERT INTO test.decimal (a) VALUES (-0.1); -- { clientError 69 }
INSERT INTO test.decimal (b) VALUES (0.1); -- { clientError 69 }
INSERT INTO test.decimal (b) VALUES (-0.1); -- { clientError 69 }
INSERT INTO test.decimal (c) VALUES (0.1); -- { clientError 69 }
INSERT INTO test.decimal (c) VALUES (-0.1); -- { clientError 69 }
INSERT INTO test.decimal (d) VALUES (0.0000000001); -- { clientError 69 }
INSERT INTO test.decimal (d) VALUES (-0.0000000001); -- { clientError 69 }
INSERT INTO test.decimal (e) VALUES (0.0000000000000000001); -- { clientError 69 }
INSERT INTO test.decimal (e) VALUES (-0.0000000000000000001); -- { clientError 69 }
INSERT INTO test.decimal (f) VALUES (0.000000000000000000000000000000000000001); -- { clientError 69 }
INSERT INTO test.decimal (f) VALUES (-0.000000000000000000000000000000000000001); -- { clientError 69 }
INSERT INTO test.decimal (g) VALUES (0.000001); -- { clientError 69 }
INSERT INTO test.decimal (g) VALUES (-0.000001); -- { clientError 69 }
INSERT INTO test.decimal (h) VALUES (0.0000000001); -- { clientError 69 }
INSERT INTO test.decimal (h) VALUES (-0.0000000001); -- { clientError 69 }
INSERT INTO test.decimal (i) VALUES (0.0000000000000000001); -- { clientError 69 }
INSERT INTO test.decimal (i) VALUES (-0.0000000000000000001); -- { clientError 69 }
INSERT INTO test.decimal (j) VALUES (0.1); -- { clientError 69 }
INSERT INTO test.decimal (j) VALUES (-0.1); -- { clientError 69 }

INSERT INTO test.decimal (a, b, d, g) VALUES (999999999, 999999999999999999, 0.999999999, 9999.99999);
INSERT INTO test.decimal (a, b, d, g) VALUES (-999999999, -999999999999999999, -0.999999999, -9999.99999);
INSERT INTO test.decimal (c) VALUES (99999999999999999999999999999999999999);
INSERT INTO test.decimal (c) VALUES (-99999999999999999999999999999999999999);
INSERT INTO test.decimal (f) VALUES (0.99999999999999999999999999999999999999);
INSERT INTO test.decimal (f) VALUES (-0.99999999999999999999999999999999999999);
INSERT INTO test.decimal (e, h) VALUES (0.999999999999999999, 999999999.999999999);
INSERT INTO test.decimal (e, h) VALUES (-0.999999999999999999, -999999999.999999999);
INSERT INTO test.decimal (i) VALUES (99999999999999999999.999999999999999999);
INSERT INTO test.decimal (i) VALUES (-99999999999999999999.999999999999999999);

INSERT INTO test.decimal (a, b, c, d, g, j, h) VALUES (1, 1, 1, 0.000000001, 0.00001, 1, 0.000000001);
INSERT INTO test.decimal (a, b, c, d, g, j, h) VALUES (-1, -1, -1, -0.000000001, -0.00001, -1, -0.000000001);
INSERT INTO test.decimal (e, f) VALUES (0.000000000000000001, 0.00000000000000000000000000000000000001);
INSERT INTO test.decimal (e, f) VALUES (-0.000000000000000001, -0.00000000000000000000000000000000000001);
INSERT INTO test.decimal (i) VALUES (0.000000000000000001);
INSERT INTO test.decimal (i) VALUES (-0.000000000000000001);

INSERT INTO test.decimal (a, b, c, d, e, f, g, h, i, j) VALUES (0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
INSERT INTO test.decimal (a, b, c, d, e, f, g, h, i, j) VALUES (-0, -0, -0, -0, -0, -0, -0, -0, -0, -0);
INSERT INTO test.decimal (a, b, c, d, e, f, g, h, i, j) VALUES (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
INSERT INTO test.decimal (a, b, c, d, e, f, g, h, i, j) VALUES (-0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0);

INSERT INTO test.decimal (a, b, g) VALUES ('42.00000', 42.0000000000000000000000000000000, '0.999990');
INSERT INTO test.decimal (a) VALUES ('-9x'); -- { clientError 72 }
INSERT INTO test.decimal (a) VALUES ('0x1'); -- { clientError 72 }
INSERT INTO test.decimal (a) VALUES ('1e2'); -- { clientError 72 }

SELECT * FROM test.decimal ORDER BY a, b, c, d, e, f, g, h, i, j;
DROP TABLE IF EXISTS test.decimal;
