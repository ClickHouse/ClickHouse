-- Tags: no-fasttest

DROP TABLE IF EXISTS h3_indexes;

CREATE TABLE h3_indexes (id int, start String, end String) ENGINE = Memory;

-- test values taken from h3 library test suite

INSERT INTO h3_indexes VALUES (1, '830631fffffffff','830780fffffffff');
INSERT INTO h3_indexes VALUES (2, '830631fffffffff','830783fffffffff');
INSERT INTO h3_indexes VALUES (3, '830631fffffffff','83079dfffffffff');
INSERT INTO h3_indexes VALUES (4, '830631fffffffff','830799fffffffff');
INSERT INTO h3_indexes VALUES (5, '830631fffffffff','8306f5fffffffff');
INSERT INTO h3_indexes VALUES (6, '830631fffffffff','8306e6fffffffff');
INSERT INTO h3_indexes VALUES (7, '830631fffffffff','8306e4fffffffff');
INSERT INTO h3_indexes VALUES (8, '830631fffffffff','830701fffffffff');
INSERT INTO h3_indexes VALUES (9, '830631fffffffff','830700fffffffff');
INSERT INTO h3_indexes VALUES (10, '830631fffffffff','830706fffffffff');
INSERT INTO h3_indexes VALUES (11, '830631fffffffff','830733fffffffff');
INSERT INTO h3_indexes VALUES (12, '8301a6fffffffff','830014fffffffff');
INSERT INTO h3_indexes VALUES (13, '8301a6fffffffff','830033fffffffff');
INSERT INTO h3_indexes VALUES (14, '8301a6fffffffff','830031fffffffff');
INSERT INTO h3_indexes VALUES (15, '8301a6fffffffff','830022fffffffff');
INSERT INTO h3_indexes VALUES (16, '8301a6fffffffff','830020fffffffff');
INSERT INTO h3_indexes VALUES (17, '8301a6fffffffff','830024fffffffff');
INSERT INTO h3_indexes VALUES (18, '8301a6fffffffff','830120fffffffff');
INSERT INTO h3_indexes VALUES (19, '8301a6fffffffff','830124fffffffff');
INSERT INTO h3_indexes VALUES (20, '8301a6fffffffff','8308cdfffffffff');
INSERT INTO h3_indexes VALUES (21, '8301a5fffffffff','831059fffffffff');
INSERT INTO h3_indexes VALUES (22, '8301a5fffffffff','830b2dfffffffff');
INSERT INTO h3_indexes VALUES (23, '8301a5fffffffff','830b29fffffffff');
INSERT INTO h3_indexes VALUES (24, '8301a5fffffffff','830b76fffffffff');
INSERT INTO h3_indexes VALUES (25, '8301a5fffffffff','830b43fffffffff');
INSERT INTO h3_indexes VALUES (26, '8301a5fffffffff','830b4efffffffff');
INSERT INTO h3_indexes VALUES (27, '8301a5fffffffff','830b48fffffffff');
INSERT INTO h3_indexes VALUES (28, '8301a5fffffffff','830b49fffffffff');


SELECT h3Distance(stringToH3(start), stringToH3(end)) FROM h3_indexes ORDER BY id;


DROP TABLE h3_indexes;

