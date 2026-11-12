# Learnings

- When adding new settings in `Settings.cpp`, you must also add `extern` declarations in every `.cpp` file that uses them via `Setting::name` (e.g., in the `namespace Setting` block in `executeQuery.cpp`, `ClientBase.cpp`).
- Rust crate build targets in ninja have the form `contrib/<lib>-cmake/cargo-build__ch_rust_<name>`, not just `_ch_rust_<name>`.
