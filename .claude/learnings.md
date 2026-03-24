# Learnings

- When adding new settings in `Settings.cpp`, you must also add `extern` declarations in every `.cpp` file that uses them via `Setting::name` (e.g., in the `namespace Setting` block in `executeQuery.cpp`, `ClientBase.cpp`).
- Rust crate build targets in ninja have the form `rust/workspace/cargo-build__ch_rust_<name>`, not just `_ch_rust_<name>`.
- `KeyCondition::extractAtomFromTree` has a gate check at the top: `if (atom_map.find(func_name) == std::end(atom_map)) return false;`. Any function that needs to be recognized as a key condition atom must have an entry in `atom_map`, even if its actual logic is handled by a separate code path (like the S2 functions handled by `tryAnalyzeS2Covering`). Removing "dead" atom_map entries can silently break pruning.
