DROP TABLE IF EXISTS 02725_cnf;

CREATE TABLE 02725_cnf (c0 UInt8, c1 UInt8, c2 UInt8, c3 UInt8, c4 UInt8, c5 UInt8, c6 UInt8, c7 UInt8, c8 UInt8, c9 UInt8) ENGINE = Memory;

INSERT INTO 02725_cnf VALUES (0, 0, 0, 0, 0, 0, 0, 0, 0, 0), (0, 0, 0, 0, 0, 0, 0, 0, 0, 1), (0, 0, 0, 0, 0, 0, 0, 0, 1, 0), (0, 0, 0, 0, 0, 0, 0, 0, 1, 1), (0, 0, 0, 0, 0, 0, 0, 1, 0, 0), (0, 0, 0, 0, 0, 0, 0, 1, 0, 1), (0, 0, 0, 0, 0, 0, 0, 1, 1, 0), (0, 0, 0, 0, 0, 0, 0, 1, 1, 1);

SELECT count()
FROM 02725_cnf
WHERE (c5 AND (NOT c0)) OR ((NOT c3) AND (NOT c6) AND (NOT c1) AND (NOT c6)) OR (c7 AND (NOT c3) AND (NOT c5) AND (NOT c7)) OR ((NOT c8) AND c5) OR ((NOT c0)) OR ((NOT c8) AND (NOT c5) AND c1 AND c6 AND c3) OR (c7 AND (NOT c0) AND c6 AND c1 AND (NOT c2)) OR (c3 AND (NOT c9) AND c1)
SETTINGS convert_query_to_cnf = 1, enable_analyzer = 1;

SELECT count()
FROM 02725_cnf
WHERE (c5 AND (NOT c0)) OR ((NOT c3) AND (NOT c6) AND (NOT c1) AND (NOT c6)) OR (c7 AND (NOT c3) AND (NOT c5) AND (NOT c7)) OR ((NOT c8) AND c5) OR ((NOT c0)) OR ((NOT c8) AND (NOT c5) AND c1 AND c6 AND c3) OR (c7 AND (NOT c0) AND c6 AND c1 AND (NOT c2)) OR (c3 AND (NOT c9) AND c1)
SETTINGS convert_query_to_cnf = 1, enable_analyzer = 0;

SELECT count()
FROM 02725_cnf
WHERE ((NOT c2) AND c2 AND (NOT c1)) OR ((NOT c2) AND c3 AND (NOT c5)) OR ((NOT c7) AND (NOT c8)) OR (c9 AND c6 AND c8 AND (NOT c8) AND (NOT c7))
SETTINGS convert_query_to_cnf = 1, enable_analyzer = 1;

SELECT count()
FROM 02725_cnf
WHERE ((NOT c2) AND c2 AND (NOT c1)) OR ((NOT c2) AND c3 AND (NOT c5)) OR ((NOT c7) AND (NOT c8)) OR (c9 AND c6 AND c8 AND (NOT c8) AND (NOT c7))
SETTINGS convert_query_to_cnf = 1, enable_analyzer = 0;

DROP TABLE 02725_cnf;
