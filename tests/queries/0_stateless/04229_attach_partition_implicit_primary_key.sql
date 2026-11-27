-- ATTACH PARTITION must succeed when the two tables have the same effective primary
-- key, regardless of whether each one declared it explicitly via PRIMARY KEY or
-- implicitly (taken from ORDER BY).

DROP TABLE IF EXISTS t_explicit_pk;
DROP TABLE IF EXISTS t_implicit_pk;
DROP TABLE IF EXISTS t_partial_pk;
DROP TABLE IF EXISTS t_implicit_pk_2;

-- Explicit PRIMARY KEY equal to ORDER BY.
CREATE TABLE t_explicit_pk (id Int32, v Int32) ENGINE = MergeTree PARTITION BY id PRIMARY KEY id ORDER BY id;
-- Implicit primary key (taken from ORDER BY).
CREATE TABLE t_implicit_pk (id Int32, v Int32) ENGINE = MergeTree PARTITION BY id ORDER BY id;

INSERT INTO t_explicit_pk VALUES (1, 10);
INSERT INTO t_implicit_pk VALUES (2, 20);

-- Both directions should work.
ALTER TABLE t_implicit_pk ATTACH PARTITION 1 FROM t_explicit_pk;
ALTER TABLE t_explicit_pk ATTACH PARTITION 2 FROM t_implicit_pk;

SELECT id, v FROM t_implicit_pk ORDER BY id, v;
SELECT id, v FROM t_explicit_pk ORDER BY id, v;

-- Real mismatch must still be rejected: explicit PRIMARY KEY is a prefix of ORDER BY,
-- so the implicit primary key (= full ORDER BY) differs from the explicit one.
CREATE TABLE t_partial_pk (id Int32, v Int32) ENGINE = MergeTree PARTITION BY id PRIMARY KEY id ORDER BY (id, v);
CREATE TABLE t_implicit_pk_2 (id Int32, v Int32) ENGINE = MergeTree PARTITION BY id ORDER BY (id, v);

INSERT INTO t_partial_pk VALUES (3, 30);

-- t_implicit_pk_2 has primary key (id, v); t_partial_pk has primary key (id). Must fail.
ALTER TABLE t_implicit_pk_2 ATTACH PARTITION 3 FROM t_partial_pk; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_explicit_pk;
DROP TABLE t_implicit_pk;
DROP TABLE t_partial_pk;
DROP TABLE t_implicit_pk_2;
