-- A table created from `generateRandom` without a structure argument generates data for the columns it was created
-- with: the ones the `CREATE` query gives, or the ones inferred then. It used to generate another random structure on
-- each read, so reading it failed, e.g. with `NO_SUCH_COLUMN_IN_TABLE`.

DROP TABLE IF EXISTS t_given;
DROP TABLE IF EXISTS t_inferred;
DROP TABLE IF EXISTS t_seeded;

CREATE TABLE t_given (a Nullable(UInt8), b String) AS generateRandom();
SELECT toTypeName(a), toTypeName(b) FROM t_given LIMIT 1;
-- `toTypeName` can be computed from the header alone: read the columns.
SELECT a, b FROM t_given LIMIT 1 FORMAT Null;

-- A `NULL` seed keeps the structure random; the sizes of strings and arrays are bounded.
CREATE TABLE t_inferred AS generateRandom(NULL, 10, 2);
SELECT count() FROM (SELECT * FROM t_inferred LIMIT 5);
SELECT count() FROM (SELECT * FROM t_inferred LIMIT 5);
SELECT * FROM t_inferred LIMIT 5 FORMAT Null;

-- With a seed alone, the seed does not decide the structure of a table with given columns either.
CREATE TABLE t_seeded (a UInt8) AS generateRandom(42);
SELECT toTypeName(a) FROM t_seeded LIMIT 1;
SELECT a FROM t_seeded LIMIT 1 FORMAT Null;

DETACH TABLE t_given PERMANENTLY;
ATTACH TABLE t_given;
DETACH TABLE t_inferred PERMANENTLY;
ATTACH TABLE t_inferred;
DETACH TABLE t_seeded PERMANENTLY;
ATTACH TABLE t_seeded;

SELECT toTypeName(a), toTypeName(b) FROM t_given LIMIT 1;
SELECT a, b FROM t_given LIMIT 1 FORMAT Null;
SELECT count() FROM (SELECT * FROM t_inferred LIMIT 5);
SELECT * FROM t_inferred LIMIT 5 FORMAT Null;
SELECT toTypeName(a) FROM t_seeded LIMIT 1;
SELECT a FROM t_seeded LIMIT 1 FORMAT Null;

DROP TABLE t_given;
DROP TABLE t_inferred;
DROP TABLE t_seeded;
