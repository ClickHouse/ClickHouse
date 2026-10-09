-- The obsolete setting is still accepted (does nothing) for backward compatibility.
SET allow_experimental_codecs = 1;

-- It no longer enables an experimental codec.
CREATE TABLE t_obsolete_umbrella (x UInt64 CODEC(ZXC)) ENGINE = MergeTree ORDER BY x; -- { serverError BAD_ARGUMENTS }

-- The setting is reported as obsolete.
SELECT name, tier, is_obsolete FROM system.settings WHERE name = 'allow_experimental_codecs';
