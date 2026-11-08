-- Tags: no-async-insert
-- - no-async-insert -- due to INSERT is performed in background the connection is preserved, and last CREATE TEMPORARY TABLE will fail with TABLE_ALREADY_EXISTS

DROP TABLE IF EXISTS default_constraints;
CREATE TABLE default_constraints
(
    x UInt8,
    y UInt8 DEFAULT x + 1,
    CONSTRAINT c CHECK y < 5
) ENGINE = Memory;

INSERT INTO default_constraints (x) SELECT number FROM system.numbers LIMIT 5; -- { serverError VIOLATED_CONSTRAINT }
INSERT INTO default_constraints (x) VALUES (0),(1),(2),(3),(4); -- { serverError VIOLATED_CONSTRAINT }

SELECT y, throwIf(NOT y < 5) FROM default_constraints;
SELECT count() FROM default_constraints;

DROP TABLE default_constraints;


CREATE TEMPORARY TABLE default_constraints
(
    x UInt8,
    y UInt8 DEFAULT x + 1,
    CONSTRAINT c CHECK y < 5
);

INSERT INTO default_constraints (x) SELECT number FROM system.numbers LIMIT 5; -- { serverError VIOLATED_CONSTRAINT }
INSERT INTO default_constraints (x) VALUES (0),(1),(2),(3),(4); -- { serverError VIOLATED_CONSTRAINT }

-- On the previous INSERT the connection got terminated
CREATE TEMPORARY TABLE default_constraints
(
    x UInt8,
    y UInt8 DEFAULT x + 1,
    CONSTRAINT c CHECK y < 5
);

SELECT y, throwIf(NOT y < 5) FROM default_constraints;
SELECT count() FROM default_constraints;
