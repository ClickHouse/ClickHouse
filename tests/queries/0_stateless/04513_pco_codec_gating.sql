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
