DROP TABLE IF EXISTS decimal;

CREATE TABLE IF NOT EXISTS decimal (x DECIMAL(10, -2)) ENGINE = Memory; -- { serverError 69 }
CREATE TABLE IF NOT EXISTS decimal (x DECIMAL(10, 15)) ENGINE = Memory; -- { serverError 69 }
CREATE TABLE IF NOT EXISTS decimal (x DECIMAL(0, 0)) ENGINE = Memory; -- { serverError 69 }

CREATE TABLE IF NOT EXISTS decimal
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

INSERT INTO decimal (a) VALUES (1000000000); -- { clientError 69 }
INSERT INTO decimal (a) VALUES (-1000000000); -- { clientError 69 }
INSERT INTO decimal (b) VALUES (1000000000000000000); -- { clientError 69 }
INSERT INTO decimal (b) VALUES (-1000000000000000000); -- { clientError 69 }
INSERT INTO decimal (c) VALUES (100000000000000000000000000000000000000); -- { clientError 69 }
INSERT INTO decimal (c) VALUES (-100000000000000000000000000000000000000); -- { clientError 69 }
INSERT INTO decimal (d) VALUES (1); -- { clientError 69 }
INSERT INTO decimal (d) VALUES (-1); -- { clientError 69 }
INSERT INTO decimal (e) VALUES (1000000000000000000); -- { clientError 69 }
INSERT INTO decimal (e) VALUES (-1000000000000000000); -- { clientError 69 }
INSERT INTO decimal (f) VALUES (1); -- { clientError 69 }
INSERT INTO decimal (f) VALUES (-1); -- { clientError 69 }
INSERT INTO decimal (g) VALUES (10000); -- { clientError 69 }
INSERT INTO decimal (g) VALUES (-10000); -- { clientError 69 }
INSERT INTO decimal (h) VALUES (1000000000); -- { clientError 69 }
INSERT INTO decimal (h) VALUES (-1000000000); -- { clientError 69 }
INSERT INTO decimal (i) VALUES (100000000000000000000); -- { clientError 69 }
INSERT INTO decimal (i) VALUES (-100000000000000000000); -- { clientError 69 }
INSERT INTO decimal (j) VALUES (10); -- { clientError 69 }
INSERT INTO decimal (j) VALUES (-10); -- { clientError 69 }

INSERT INTO decimal (a) VALUES (0.1);
INSERT INTO decimal (a) VALUES (-0.1);
INSERT INTO decimal (b) VALUES (0.1);
INSERT INTO decimal (b) VALUES (-0.1);
INSERT INTO decimal (c) VALUES (0.1);
INSERT INTO decimal (c) VALUES (-0.1);
INSERT INTO decimal (d) VALUES (0.0000000001);
INSERT INTO decimal (d) VALUES (-0.0000000001);
INSERT INTO decimal (e) VALUES (0.0000000000000000001);
INSERT INTO decimal (e) VALUES (-0.0000000000000000001);
INSERT INTO decimal (f) VALUES (0.000000000000000000000000000000000000001);
INSERT INTO decimal (f) VALUES (-0.000000000000000000000000000000000000001);
INSERT INTO decimal (g) VALUES (0.000001);
INSERT INTO decimal (g) VALUES (-0.000001);
INSERT INTO decimal (h) VALUES (0.0000000001);
INSERT INTO decimal (h) VALUES (-0.0000000001);
INSERT INTO decimal (i) VALUES (0.0000000000000000001);
INSERT INTO decimal (i) VALUES (-0.0000000000000000001);
INSERT INTO decimal (j) VALUES (0.1);
INSERT INTO decimal (j) VALUES (-0.1);

INSERT INTO decimal (a, b, d, g) VALUES (999999999, 999999999999999999, 0.999999999, 9999.99999);
INSERT INTO decimal (a, b, d, g) VALUES (-999999999, -999999999999999999, -0.999999999, -9999.99999);
INSERT INTO decimal (c) VALUES (99999999999999999999999999999999999999);
INSERT INTO decimal (c) VALUES (-99999999999999999999999999999999999999);
INSERT INTO decimal (f) VALUES (0.99999999999999999999999999999999999999);
INSERT INTO decimal (f) VALUES (-0.99999999999999999999999999999999999999);
INSERT INTO decimal (e, h) VALUES (0.999999999999999999, 999999999.999999999);
INSERT INTO decimal (e, h) VALUES (-0.999999999999999999, -999999999.999999999);
INSERT INTO decimal (i) VALUES (99999999999999999999.999999999999999999);
INSERT INTO decimal (i) VALUES (-99999999999999999999.999999999999999999);

INSERT INTO decimal (a, b, c, d, g, j, h) VALUES (1, 1, 1, 0.000000001, 0.00001, 1, 0.000000001);
INSERT INTO decimal (a, b, c, d, g, j, h) VALUES (-1, -1, -1, -0.000000001, -0.00001, -1, -0.000000001);
INSERT INTO decimal (e, f) VALUES (0.000000000000000001, 0.00000000000000000000000000000000000001);
INSERT INTO decimal (e, f) VALUES (-0.000000000000000001, -0.00000000000000000000000000000000000001);
INSERT INTO decimal (i) VALUES (0.000000000000000001);
INSERT INTO decimal (i) VALUES (-0.000000000000000001);

INSERT INTO decimal (a, b, c, d, e, f, g, h, i, j) VALUES (0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
INSERT INTO decimal (a, b, c, d, e, f, g, h, i, j) VALUES (-0, -0, -0, -0, -0, -0, -0, -0, -0, -0);
INSERT INTO decimal (a, b, c, d, e, f, g, h, i, j) VALUES (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
INSERT INTO decimal (a, b, c, d, e, f, g, h, i, j) VALUES (-0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0, -0.0);

INSERT INTO decimal (a, b, g) VALUES ('42.00000', 42.0000000000000000000000000000000, '0.999990');
INSERT INTO decimal (a) VALUES ('-9x'); -- { clientError 6 }
INSERT INTO decimal (a) VALUES ('0x1'); -- { clientError 6 }

INSERT INTO decimal (a, b, c, d, e, f) VALUES ('0.9e9', '0.9e18', '0.9e38', '9e-9', '9e-18', '9e-38');
INSERT INTO decimal (a, b, c, d, e, f) VALUES ('-0.9e9', '-0.9e18', '-0.9e38', '-9e-9', '-9e-18', '-9e-38');

INSERT INTO decimal (a, b, c, d, e, f) VALUES ('1e9', '1e18', '1e38', '1e-10', '1e-19', '1e-39'); -- { clientError 69 }
INSERT INTO decimal (a, b, c, d, e, f) VALUES ('-1e9', '-1e18', '-1e38', '-1e-10', '-1e-19', '-1e-39'); -- { clientError 69 }

SELECT * FROM decimal ORDER BY a, b, c, d, e, f, g, h, i, j;
DROP TABLE IF EXISTS decimal;
