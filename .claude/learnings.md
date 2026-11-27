# Learnings

- When adding new settings in `Settings.cpp`, you must also add `extern` declarations in every `.cpp` file that uses them via `Setting::name` (e.g., in the `namespace Setting` block in `executeQuery.cpp`, `ClientBase.cpp`).
- All Rust crates are built by one ninja target, `contrib/rust-contrib-workspace/cargo-build__ch_rust_workspace`; `_ch_rust_<name>` are only interface targets.
