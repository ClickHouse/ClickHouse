# Learnings

- When adding new settings in `Settings.cpp`, you must also add `extern` declarations in every `.cpp` file that uses them via `Setting::name` (e.g., in the `namespace Setting` block in `executeQuery.cpp`, `ClientBase.cpp`).
- Rust crate build targets in ninja have the form `rust/workspace/cargo-build__ch_rust_<name>`, not just `_ch_rust_<name>`.
- When manually feeding columns to aggregate functions (outside the standard `Aggregator`), always call `convertToFullIfNeeded()` to handle `ColumnConst`, `ColumnReplicated`, `ColumnSparse`, and `ColumnLowCardinality` wrappers. The standard `Aggregator` does `convertToFullColumnIfConst()->convertToFullColumnIfReplicated()`. Flaky checks with random settings (e.g., `max_block_size=1`) can produce these column types unexpectedly.
