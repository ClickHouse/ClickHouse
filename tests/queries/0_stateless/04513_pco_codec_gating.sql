-- Tags: no-fasttest
-- Gating of the experimental PCO (pcodec) codec: the experimental switch, the fact that it needs a
-- column type (so it is rejected in TTL RECOMPRESS), and that a codec-only ALTER can still add it.

-- Experimental gate: rejected by default, both directly and inside a chain.
DROP TABLE IF EXISTS t_pco_gate;
CREATE TABLE t_pco_gate (x UInt32 CODEC(PCO)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_pco_gate (x UInt32 CODEC(Delta, PCO)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

SET allow_experimental_codecs = 1;

-- Allowed once the switch is on.
CREATE TABLE t_pco_gate (x UInt32 CODEC(PCO)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_pco_gate SELECT number FROM numbers(1000);
SELECT 'rows', count() FROM t_pco_gate;
DROP TABLE t_pco_gate;

-- PCO needs the column type, so it cannot be used where the codec is resolved without one:
-- TTL ... RECOMPRESS (rejected even with the experimental switch on).
DROP TABLE IF EXISTS t_pco_ttl;
CREATE TABLE t_pco_ttl (d Date, x UInt32)
ENGINE = MergeTree ORDER BY tuple()
TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(PCO); -- { serverError BAD_ARGUMENTS }

-- `allow_suspicious_ttl_expressions` only relaxes the "TTL expression must depend on table columns" check;
-- it is not a codec escape hatch and does not turn a `CREATE` / `ALTER ... MODIFY TTL` into a metadata load,
-- so PCO in TTL RECOMPRESS is still rejected (not silently normalized to the default codec) at DDL time.
SET allow_suspicious_ttl_expressions = 1;
CREATE TABLE t_pco_ttl (d Date, x UInt32)
ENGINE = MergeTree ORDER BY tuple()
TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(PCO); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_pco_alter_ttl (d Date, x UInt32) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_pco_alter_ttl MODIFY TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(PCO); -- { serverError BAD_ARGUMENTS }
DROP TABLE t_pco_alter_ttl;
SET allow_suspicious_ttl_expressions = 0;

-- The untyped MergeTree compression settings reject PCO (experimental, and marks/primary-key
-- streams are also untyped), both directly and inside a chain, even with the switch on.
CREATE TABLE t_pco_s (x UInt32) ENGINE = MergeTree ORDER BY tuple() SETTINGS default_compression_codec = 'PCO'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_pco_s (x UInt32) ENGINE = MergeTree ORDER BY tuple() SETTINGS marks_compression_codec = 'PCO'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_pco_s (x UInt32) ENGINE = MergeTree ORDER BY tuple() SETTINGS primary_key_compression_codec = 'PCO'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_pco_s (x UInt32) ENGINE = MergeTree ORDER BY tuple() SETTINGS default_compression_codec = 'PCO, ZSTD(1)'; -- { serverError BAD_ARGUMENTS }
-- A normal codec in the same setting is still accepted.
CREATE TABLE t_pco_s (x UInt32) ENGINE = MergeTree ORDER BY tuple() SETTINGS marks_compression_codec = 'ZSTD(3)';
INSERT INTO t_pco_s VALUES (1);
SELECT 'normal_marks_codec', count() FROM t_pco_s;
DROP TABLE t_pco_s;

-- A codec-only ALTER MODIFY COLUMN (the type is not restated) can still add PCO: the existing
-- column type is used for validation.
DROP TABLE IF EXISTS t_pco_alter;
CREATE TABLE t_pco_alter (id UInt64, x Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_pco_alter SELECT number, number * 3 - 7 FROM numbers(5000);
ALTER TABLE t_pco_alter MODIFY COLUMN x CODEC(PCO);
OPTIMIZE TABLE t_pco_alter FINAL;
SELECT 'alter_added_pco', countIf(x != toInt64(id * 3 - 7)) FROM t_pco_alter;
SELECT 'alter_codec', compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 't_pco_alter' AND name = 'x';
DROP TABLE t_pco_alter;
