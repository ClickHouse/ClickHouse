-- Tags: no-fasttest
-- Adding `PCO` to an existing numeric column via a codec-only `ALTER TABLE ... MODIFY COLUMN x CODEC(PCO)`
-- (where the type is not restated) must work. `PCO` needs the column type to compress, so the validation
-- has to fall back to the existing column type, matching `AlterCommand::apply`, instead of rejecting it as
-- an untyped codec.

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS t_pco_alter;

CREATE TABLE t_pco_alter (id UInt64, x UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_pco_alter SELECT number, number * 3 FROM numbers(1000);

-- Codec-only MODIFY COLUMN: the type is not restated, so `command.data_type` is null and the validation
-- must use the existing `UInt64` type instead of rejecting `PCO` as an untyped codec.
ALTER TABLE t_pco_alter MODIFY COLUMN x CODEC(PCO);
SELECT compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 't_pco_alter' AND name = 'x';

-- Rewrite existing parts so they are actually re-encoded with `PCO`, then verify the data is intact.
OPTIMIZE TABLE t_pco_alter FINAL;
SELECT sum(x != id * 3) FROM t_pco_alter;

-- New writes also round-trip with the column's `PCO` codec.
INSERT INTO t_pco_alter SELECT number, number * 3 FROM numbers(1000, 1000);
SELECT count(), sum(x != id * 3) FROM t_pco_alter;

-- A codec-only ALTER that restates the type, with `PCO` inside a chain, is accepted as well.
ALTER TABLE t_pco_alter MODIFY COLUMN x UInt64 CODEC(Delta, PCO);
SELECT compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 't_pco_alter' AND name = 'x';

DROP TABLE t_pco_alter;

-- `PCO` is still experimental: a codec-only ALTER must be rejected without `allow_experimental_codecs`.
CREATE TABLE t_pco_alter (id UInt64, x UInt64, s String) ENGINE = MergeTree ORDER BY id;
SET allow_experimental_codecs = 0;
ALTER TABLE t_pco_alter MODIFY COLUMN x CODEC(PCO); -- { serverError BAD_ARGUMENTS }
SET allow_experimental_codecs = 1;

-- `PCO` requires a fixed-width numeric type: adding it to a String column must be rejected even though
-- the type is taken from the existing column.
ALTER TABLE t_pco_alter MODIFY COLUMN s CODEC(PCO); -- { serverError BAD_ARGUMENTS }

DROP TABLE t_pco_alter;
