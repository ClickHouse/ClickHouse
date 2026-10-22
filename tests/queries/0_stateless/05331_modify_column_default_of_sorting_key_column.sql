-- Parts written before a column was added to the sorting key do not store it, so readers compute it from its
-- DEFAULT or MATERIALIZED expression. Changing that expression is allowed only if it keeps those parts sorted.

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_expr;
DROP TABLE IF EXISTS t_name;
DROP TABLE IF EXISTS t_f;
DROP TABLE IF EXISTS t_n;
DROP TABLE IF EXISTS t_dep;
DROP TABLE IF EXISTS t_b;
DROP TABLE IF EXISTS t_i;
DROP TABLE IF EXISTS t_p;
DROP TABLE IF EXISTS t_v;
DROP TABLE IF EXISTS t_r;
DROP TABLE IF EXISTS t_c;
DROP TABLE IF EXISTS t_sub;
DROP TABLE IF EXISTS t_s;

CREATE TABLE t (a UInt32, x UInt32, y UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t VALUES (1, 30, 10), (1, 10, 1);
INSERT INTO t VALUES (2, 5, 5);
ALTER TABLE t ADD COLUMN k UInt32, MODIFY ORDER BY (a, k);
ALTER TABLE t MODIFY COLUMN k UInt32 DEFAULT x + y; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t MODIFY COLUMN k UInt32 MATERIALIZED x + y; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t MODIFY COLUMN k UInt32 DEFAULT rand(); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t MODIFY COLUMN k UInt32 DEFAULT a + rand(); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- A constant, an expression of primary key columns, and a default of a primary key column are allowed.
ALTER TABLE t MODIFY COLUMN k UInt32 DEFAULT 7;
ALTER TABLE t MODIFY COLUMN k UInt32 DEFAULT a * 10;
ALTER TABLE t MODIFY COLUMN a UInt32 DEFAULT x + y;
SELECT groupArray((a, k)) FROM (SELECT a, k FROM t ORDER BY a, k);
SELECT groupArray((a, k)) FROM (SELECT a, k FROM t ORDER BY a, k SETTINGS optimize_read_in_order = 0);
OPTIMIZE TABLE t FINAL;
SELECT groupArray((a, k)) FROM (SELECT a, k FROM t ORDER BY a, k);

-- Reading the primary key through an ALIAS is allowed, changing that ALIAS afterwards is not.
ALTER TABLE t ADD COLUMN p UInt32 ALIAS a;
ALTER TABLE t MODIFY COLUMN k UInt32 DEFAULT p;
ALTER TABLE t MODIFY COLUMN p UInt32 ALIAS x + y; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t ADD COLUMN pr UInt32 ALIAS rand();
ALTER TABLE t MODIFY COLUMN k UInt32 DEFAULT pr; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t ADD COLUMN e UInt32 EPHEMERAL a;
ALTER TABLE t MODIFY COLUMN k UInt32 DEFAULT e; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- The final state of the ALTER decides.
ALTER TABLE t MODIFY COLUMN k UInt32 DEFAULT x + y, MODIFY COLUMN k REMOVE DEFAULT;
ALTER TABLE t MODIFY COLUMN k UInt32 DEFAULT a;
ALTER TABLE t MODIFY COLUMN k REMOVE DEFAULT;

-- `a` is not a primary key column as it is, only through an expression.
CREATE TABLE t_expr (a UInt32, x UInt32) ENGINE = MergeTree ORDER BY intDiv(a, 10);
ALTER TABLE t_expr ADD COLUMN k UInt32, MODIFY ORDER BY (intDiv(a, 10), k);
ALTER TABLE t_expr MODIFY COLUMN k UInt32 DEFAULT a; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- A column named like a primary key expression is not that expression.
CREATE TABLE t_name (a UInt32, `intDiv(a, 10)` UInt32) ENGINE = MergeTree ORDER BY intDiv(a, 10);
ALTER TABLE t_name ADD COLUMN k UInt32, MODIFY ORDER BY (intDiv(a, 10), k);
ALTER TABLE t_name MODIFY COLUMN k UInt32 DEFAULT `intDiv(a, 10)`; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- Rows with equal Float or Nullable primary key values can still differ.
CREATE TABLE t_f (a Float64, x UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_f VALUES (-0., 1), (0., 2);
ALTER TABLE t_f ADD COLUMN k UInt64, MODIFY ORDER BY (a, k);
ALTER TABLE t_f MODIFY COLUMN k UInt64 DEFAULT reinterpretAsUInt64(a); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_f MODIFY COLUMN k UInt64 DEFAULT 1;

CREATE TABLE t_n (a Nullable(UInt32), x UInt32) ENGINE = MergeTree ORDER BY a SETTINGS allow_nullable_key = 1;
ALTER TABLE t_n ADD COLUMN k UInt32, MODIFY ORDER BY (a, k);
ALTER TABLE t_n MODIFY COLUMN k UInt32 DEFAULT assumeNotNull(a); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- Changing the default of a primary key column, or of what it reads, keeps an allowed expression allowed.
CREATE TABLE t_dep (a UInt32 DEFAULT x + 1, x UInt32) ENGINE = MergeTree ORDER BY a;
ALTER TABLE t_dep ADD COLUMN k UInt32, MODIFY ORDER BY (a, k);
ALTER TABLE t_dep MODIFY COLUMN k UInt32 DEFAULT a * 2;
ALTER TABLE t_dep MODIFY COLUMN x UInt32 DEFAULT 5;
ALTER TABLE t_dep MODIFY COLUMN a UInt32 DEFAULT x + 2;

-- The sorting key columns before the column can be read too, but no column after it or outside the sorting key.
CREATE TABLE t_b (a UInt32, x UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_b VALUES (1, 30), (1, 10), (2, 5);
ALTER TABLE t_b ADD COLUMN b UInt32, MODIFY ORDER BY (a, b);
INSERT INTO t_b (a, x, b) VALUES (1, 7, 9), (1, 8, 3);
ALTER TABLE t_b ADD COLUMN k UInt32, MODIFY ORDER BY (a, b, k);
ALTER TABLE t_b MODIFY COLUMN b UInt32 DEFAULT a;
ALTER TABLE t_b MODIFY COLUMN k UInt32 DEFAULT b;
ALTER TABLE t_b MODIFY COLUMN x UInt32 DEFAULT a;
ALTER TABLE t_b MODIFY COLUMN k UInt32 DEFAULT x; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT groupArray((a, b, k)) FROM (SELECT a, b, k FROM t_b ORDER BY a, b, k);
OPTIMIZE TABLE t_b FINAL;
SELECT groupArray((a, b, k)) FROM (SELECT a, b, k FROM t_b ORDER BY a, b, k);

CREATE TABLE t_i (a UInt32, c UInt32) ENGINE = MergeTree ORDER BY (a, c) PRIMARY KEY a;
INSERT INTO t_i VALUES (1, 1), (1, 2);
ALTER TABLE t_i ADD COLUMN k Int64, MODIFY ORDER BY (a, k, c);
ALTER TABLE t_i MODIFY COLUMN k Int64 DEFAULT -c; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- Partition key columns and the engine's version, is_deleted and sign columns exist since CREATE, so every part stores them.
CREATE TABLE t_p (a UInt32, d Date, x UInt32) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY (a, d) PRIMARY KEY a;
ALTER TABLE t_p MODIFY COLUMN d Date DEFAULT toDate(x);
CREATE TABLE t_v (a UInt32, x UInt32, s Int8, v UInt32) ENGINE = VersionedCollapsingMergeTree(s, v) ORDER BY a;
ALTER TABLE t_v MODIFY COLUMN v UInt32 DEFAULT x + 1;
CREATE TABLE t_r (a UInt32, x UInt32, v UInt32, d UInt8) ENGINE = ReplacingMergeTree(v, d) ORDER BY (a, v, d) PRIMARY KEY a;
ALTER TABLE t_r MODIFY COLUMN v UInt32 DEFAULT x + 1;
ALTER TABLE t_r MODIFY COLUMN d UInt8 DEFAULT x % 2;
CREATE TABLE t_c (a UInt32, x UInt32, s Int8 DEFAULT if(x > 0, 1, -1)) ENGINE = CollapsingMergeTree(s) ORDER BY (a, s) PRIMARY KEY a;
ALTER TABLE t_c MODIFY COLUMN x UInt32 DEFAULT 5;

-- An expression that already breaks the rule, like one from CREATE, can still change.
CREATE TABLE t_s (a UInt32, k UInt32 DEFAULT x, x UInt32) ENGINE = MergeTree ORDER BY (a, k) PRIMARY KEY a;
INSERT INTO t_s (a, k, x) VALUES (1, 10, 30), (1, 20, 10);
ALTER TABLE t_s MODIFY COLUMN k UInt32 DEFAULT x + 1;
ALTER TABLE t_s MODIFY COLUMN x UInt32 DEFAULT 3;
INSERT INTO t_s (a) VALUES (1);
SELECT groupArray((a, k)) FROM (SELECT a, k FROM t_s ORDER BY a, k);

-- A sorting key column used through a subcolumn.
CREATE TABLE t_sub (a UInt32, x UInt32) ENGINE = MergeTree ORDER BY a;
ALTER TABLE t_sub ADD COLUMN t Tuple(p UInt32), MODIFY ORDER BY (a, t.p);
ALTER TABLE t_sub MODIFY COLUMN t Tuple(p UInt32) DEFAULT tuple(x); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t;
DROP TABLE t_expr;
DROP TABLE t_name;
DROP TABLE t_f;
DROP TABLE t_n;
DROP TABLE t_dep;
DROP TABLE t_b;
DROP TABLE t_i;
DROP TABLE t_p;
DROP TABLE t_v;
DROP TABLE t_r;
DROP TABLE t_c;
DROP TABLE t_sub;
DROP TABLE t_s;
