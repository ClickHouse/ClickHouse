-- A table created from `generateRandom` without a structure argument generates data for the columns it was created
-- with: the ones the `CREATE` query gives, or the ones inferred then. It used to generate another random structure on
-- each read, so reading it failed, e.g. with `NO_SUCH_COLUMN_IN_TABLE`.

DROP TABLE IF EXISTS t_given;
DROP TABLE IF EXISTS t_inferred;

CREATE TABLE t_given (a Nullable(UInt8), b String) AS generateRandom();
SELECT toTypeName(a), toTypeName(b) FROM t_given LIMIT 1;
-- `toTypeName` can be computed from the header alone: read the columns.
SELECT a, b FROM t_given LIMIT 1 FORMAT Null;

CREATE TABLE t_inferred AS generateRandom();
SELECT count() FROM (SELECT * FROM t_inferred LIMIT 5);
SELECT count() FROM (SELECT * FROM t_inferred LIMIT 5);
SELECT * FROM t_inferred LIMIT 5 FORMAT Null;

DETACH TABLE t_given PERMANENTLY;
ATTACH TABLE t_given;
DETACH TABLE t_inferred PERMANENTLY;
ATTACH TABLE t_inferred;

SELECT toTypeName(a), toTypeName(b) FROM t_given LIMIT 1;
SELECT a, b FROM t_given LIMIT 1 FORMAT Null;
SELECT count() FROM (SELECT * FROM t_inferred LIMIT 5);
SELECT * FROM t_inferred LIMIT 5 FORMAT Null;

DROP TABLE t_given;
DROP TABLE t_inferred;
