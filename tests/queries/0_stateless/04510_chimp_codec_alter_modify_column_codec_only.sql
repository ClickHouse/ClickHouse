-- Regression test: a codec-only `ALTER TABLE ... MODIFY COLUMN x CODEC(Chimp)` (the column type is
-- not restated) must succeed for an existing `Float32`/`Float64` column.
--
-- `AlterCommands::validate` passes `command.data_type` into codec validation, and it is null when
-- the ALTER changes only the codec. The width-dependent `Chimp` codec was then rejected on the
-- no-column validation path as widthless with `ILLEGAL_CODEC_PARAMETER`, even though the column has
-- a determined type and `AlterCommand::apply` already falls back to it. The validation now uses the
-- existing column type too, so a codec-only `MODIFY COLUMN` is accepted and resolves the width from
-- the column, matching the behaviour of the width-dependent `Delta` codec (00804_test_delta_codec_no_type_alter).

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS chimp_codec_alter;
CREATE TABLE chimp_codec_alter (x Float64, y Float32) ENGINE = MergeTree ORDER BY tuple();

-- Codec-only `MODIFY COLUMN` without restating the type: the width is resolved from the existing
-- column type (8 bytes for `Float64`, 4 bytes for `Float32`).
ALTER TABLE chimp_codec_alter MODIFY COLUMN x CODEC(Chimp);
ALTER TABLE chimp_codec_alter MODIFY COLUMN y CODEC(Chimp, LZ4);
SELECT name, compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 'chimp_codec_alter' ORDER BY name;

-- Restating the type explicitly is still accepted.
ALTER TABLE chimp_codec_alter MODIFY COLUMN x Float64 CODEC(Chimp, ZSTD(1));
SELECT name, compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 'chimp_codec_alter' AND name = 'x';

-- The altered codec is actually usable: round-trip data through it.
INSERT INTO chimp_codec_alter SELECT number, number FROM numbers(1000);
SELECT count(), sum(x), sum(y) FROM chimp_codec_alter;

DROP TABLE chimp_codec_alter;
