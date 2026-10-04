-- The base62 conversion is quadratic in the input length, so the input size is limited by the setting
-- function_base62_max_input_size (10 KB by default). With the limit disabled, cancellation is checked
-- periodically inside the conversion, driven by the accumulated inner-loop work.
--
-- The leading-zero scans are a special path: an input of only zero bytes (for encoding) or only "0"
-- characters (for decoding) never accumulates any inner-loop work, so the scans participate in the
-- cancellation check themselves. These queries pin that behavior: several gigabytes of zero-prefix
-- data in a single block must observe the time limit from inside the conversion, not only at the
-- block boundary. (A build without the in-scan check throws the same error after the whole block is
-- processed, so the discriminating signal is the query duration; the hung-check in stress tests and
-- the test-runner timeout catch gross regressions.) The block size is limited so that the peak memory
-- stays within the query memory limit of constrained CI environments, and the result is aggregated
-- to a single value so that the result-size limits of the CI profile do not fire first (they are also
-- disabled explicitly, like in 03223_analyzer_with_cube_fuzz). QUERY_WAS_CANCELLED is accepted along
-- with TIMEOUT_EXCEEDED: on slow builds under load the 1-second deadline can expire before the query
-- leaves the pending state, in which case the kill is reported as cancellation.

-- Quadratic path: many in-limit values in a single block must respect the time limit.
SELECT base62Encode(randomString(10000)) FROM numbers(1000) FORMAT Null SETTINGS max_execution_time = 1, max_result_rows = 0, max_result_bytes = 0; -- { serverError TIMEOUT_EXCEEDED, QUERY_WAS_CANCELLED }

-- Zero-prefix path of the encoder: only zero bytes, no inner-loop work at all.
SELECT max(length(base62Encode(s))) FROM (SELECT materialize(repeat(repeat('\0', 1000), 1000)) AS s FROM numbers(6000)) FORMAT Null SETTINGS function_base62_max_input_size = 0, max_execution_time = 1, max_block_size = 500, max_result_rows = 0, max_result_bytes = 0; -- { serverError TIMEOUT_EXCEEDED, QUERY_WAS_CANCELLED }

-- Zero-prefix path of the decoder: only "0" characters, no inner-loop work at all.
SELECT max(length(base62Decode(s))) FROM (SELECT materialize(repeat(repeat('0', 1000), 1000)) AS s FROM numbers(6000)) FORMAT Null SETTINGS function_base62_max_input_size = 0, max_execution_time = 1, max_block_size = 500, max_result_rows = 0, max_result_bytes = 0; -- { serverError TIMEOUT_EXCEEDED, QUERY_WAS_CANCELLED }

SELECT 'ok';
