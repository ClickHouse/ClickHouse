-- Tags: no-fasttest, no-msan
-- Tag no-fasttest: depends on delta-kernel-rs (not built in fast test)
-- Tag no-msan: delta-kernel-rs is not built with MSan

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104509
-- and https://github.com/ClickHouse/ClickHouse/issues/105895.
--
-- A binary SAS token (e.g. raw `MD5(...)` bytes) used to abort the server because
-- the delta-kernel-rs FFI panicked on invalid UTF-8 inside `set_builder_option`,
-- crossing the `extern "C"` boundary and triggering `panic_in_cleanup`. The
-- C++ wrapper must validate the bytes and reject the input with `BAD_ARGUMENTS`
-- instead of letting the panic propagate.

SELECT * FROM deltaLakeAzure('\0', MD5('')) SETTINGS allow_experimental_delta_kernel_rs = 1; -- { serverError BAD_ARGUMENTS }
