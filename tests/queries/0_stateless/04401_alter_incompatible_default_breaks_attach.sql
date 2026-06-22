-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/59949
-- An ALTER (or CREATE) that introduces a default/alias expression whose value cannot be
-- cast to the column type must be rejected at DDL time. Previously such an ALTER succeeded
-- and the failure only surfaced later at ATTACH, which could block server startup.

DROP TABLE IF EXISTS t_alias;

CREATE TABLE t_alias (a String DEFAULT '') ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alias VALUES ('5');

-- The alias `toInt8(a)` folds to `toInt8('')`, which cannot be parsed, so the ALTER must be rejected.
ALTER TABLE t_alias ADD COLUMN quote_type Int8 ALIAS toInt8(a); -- { serverError ATTEMPT_TO_READ_AFTER_EOF }

-- The table must remain usable and unmodified after the rejected ALTER.
SELECT a FROM t_alias;

-- A valid alias still works and survives DETACH/ATTACH.
ALTER TABLE t_alias ADD COLUMN len UInt64 ALIAS length(a);
DETACH TABLE t_alias;
ATTACH TABLE t_alias;
SELECT a, len FROM t_alias;

DROP TABLE t_alias;

-- The same incompatibility introduced at CREATE time must also be rejected.
CREATE TABLE t_alias (a String DEFAULT '', quote_type Int8 ALIAS toInt8(a)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ATTEMPT_TO_READ_AFTER_EOF }

-- Breaking a dependent alias by MODIFYing the column it reads must also be rejected.
CREATE TABLE t_alias (a String DEFAULT '123', quote_type Int8 ALIAS toInt8(a)) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_alias MODIFY COLUMN a String DEFAULT ''; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }

-- The table is still attachable because the breaking ALTER never modified its metadata.
DETACH TABLE t_alias;
ATTACH TABLE t_alias;
SELECT count() FROM t_alias;

DROP TABLE t_alias;
