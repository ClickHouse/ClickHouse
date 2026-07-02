-- Tags: no-fasttest
-- `estimateCompressionRatio('<codec>')(column)` constructs the requested codec through the typed
-- factory path. For experimental codecs such as `PCO` this must still honor `allow_experimental_codecs`,
-- otherwise the aggregate function would be a way to exercise an experimental codec with the gate off.

-- With the gate off, `PCO` (and a chain wrapping it) must be rejected.
SET allow_experimental_codecs = 0;
SELECT estimateCompressionRatio('PCO')(number) FROM numbers(1000); -- { serverError BAD_ARGUMENTS }
SELECT estimateCompressionRatio('PCO, ZSTD')(number) FROM numbers(1000); -- { serverError BAD_ARGUMENTS }
SELECT estimateCompressionRatio('Delta, PCO')(number) FROM numbers(1000); -- { serverError BAD_ARGUMENTS }

-- Non-experimental codecs are unaffected by the gate.
SELECT estimateCompressionRatio('ZSTD')(number) > 0 FROM numbers(1000);
-- The sanity check is intentionally disabled here, so estimating a single transformation codec
-- (which "does not compress anything") keeps working as it did before this gate was added.
SELECT estimateCompressionRatio('Delta')(number) > 0 FROM numbers(1000);
-- The default-codec path (no codec parameter) does not depend on the setting.
SELECT estimateCompressionRatio(number) > 0 FROM numbers(1000);

-- With the gate on, `PCO` is allowed on supported numeric types.
SET allow_experimental_codecs = 1;
SELECT estimateCompressionRatio('PCO')(number) > 0 FROM numbers(1000);
SELECT estimateCompressionRatio('PCO')(toFloat64(number)) > 0 FROM numbers(1000);
SELECT estimateCompressionRatio('PCO, ZSTD')(number) > 0 FROM numbers(1000);
