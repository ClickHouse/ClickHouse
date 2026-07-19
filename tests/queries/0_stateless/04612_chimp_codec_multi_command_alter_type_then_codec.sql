-- Regression test: in a multi-command ALTER, a codec-only `MODIFY COLUMN x CODEC(Chimp)` must
-- validate against the type set by an earlier `MODIFY COLUMN x Float64` in the same statement.
--
-- `AlterCommands::validate` used to keep the original schema snapshot for the whole statement, so
-- the codec-only command resolved the column type from the original schema (e.g. `UInt64`) and
-- rejected the width-dependent `Chimp` codec, even though `AlterCommand::apply` sees the column as
-- `Float64` and accepts it. The validation snapshot now advances on type changes, matching `apply`.

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS chimp_codec_multi_alter;
CREATE TABLE chimp_codec_multi_alter (x UInt64, y UInt32) ENGINE = MergeTree ORDER BY tuple();

-- Change the type and set the codec in one ALTER: the codec must resolve its width (8) from the
-- new `Float64` type, not fail against the original `UInt64`.
ALTER TABLE chimp_codec_multi_alter MODIFY COLUMN x Float64, MODIFY COLUMN x CODEC(Chimp);
ALTER TABLE chimp_codec_multi_alter MODIFY COLUMN y Float32, MODIFY COLUMN y CODEC(Chimp, LZ4);
SELECT name, type, compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 'chimp_codec_multi_alter' ORDER BY name;

-- The conversion and the codec are actually usable: round-trip data through them.
INSERT INTO chimp_codec_multi_alter SELECT number, number FROM numbers(1000);
SELECT count(), sum(x), sum(y) FROM chimp_codec_multi_alter;

-- The opposite direction still fails: a type change away from floating point makes `Chimp`
-- meaningless for the codec-only command that follows in the same ALTER.
ALTER TABLE chimp_codec_multi_alter MODIFY COLUMN x UInt64, MODIFY COLUMN x CODEC(Chimp); -- { serverError BAD_ARGUMENTS }

DROP TABLE chimp_codec_multi_alter;
