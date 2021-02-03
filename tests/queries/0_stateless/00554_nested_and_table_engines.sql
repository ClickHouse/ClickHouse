DROP TABLE IF EXISTS nested;

CREATE TABLE nested (x UInt8, n Nested(a UInt64, b String)) ENGINE = TinyLog;

INSERT INTO nested VALUES (1, [2, 3], ['Hello', 'World']);
INSERT INTO nested VALUES (4, [5], ['Goodbye']);

SELECT * FROM nested ORDER BY x;
SELECT x, n.a FROM nested ORDER BY x;
SELECT n.a, n.b FROM nested ORDER BY n.a;


DROP TABLE IF EXISTS nested;

CREATE TABLE nested (x UInt8, n Nested(a UInt64, b String)) ENGINE = Log;

INSERT INTO nested VALUES (1, [2, 3], ['Hello', 'World']);
INSERT INTO nested VALUES (4, [5], ['Goodbye']);

SELECT * FROM nested ORDER BY x;
SELECT x, n.a FROM nested ORDER BY x;
SELECT n.a, n.b FROM nested ORDER BY n.a;


DROP TABLE IF EXISTS nested;

CREATE TABLE nested (x UInt8, n Nested(a UInt64, b String)) ENGINE = StripeLog;

INSERT INTO nested VALUES (1, [2, 3], ['Hello', 'World']);
INSERT INTO nested VALUES (4, [5], ['Goodbye']);

SELECT * FROM nested ORDER BY x;
SELECT x, n.a FROM nested ORDER BY x;
SELECT n.a, n.b FROM nested ORDER BY n.a;


DROP TABLE IF EXISTS nested;

CREATE TABLE nested (x UInt8, n Nested(a UInt64, b String)) ENGINE = Memory;

INSERT INTO nested VALUES (1, [2, 3], ['Hello', 'World']);
INSERT INTO nested VALUES (4, [5], ['Goodbye']);

SELECT * FROM nested ORDER BY x;
SELECT x, n.a FROM nested ORDER BY x;
SELECT n.a, n.b FROM nested ORDER BY n.a;


DROP TABLE IF EXISTS nested;

CREATE TABLE nested (x UInt8, n Nested(a UInt64, b String)) ENGINE = MergeTree ORDER BY x;

INSERT INTO nested VALUES (1, [2, 3], ['Hello', 'World']);
INSERT INTO nested VALUES (4, [5], ['Goodbye']);

SELECT * FROM nested ORDER BY x;
SELECT x, n.a FROM nested ORDER BY x;
SELECT n.a, n.b FROM nested ORDER BY n.a;


DROP TABLE nested;
