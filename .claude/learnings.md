# Learnings

## Add settings
- When adding new settings in `Settings.cpp`, you must also add `extern` declarations in every `.cpp` file that uses them via `Setting::name` (e.g., in the `namespace Setting` block in `executeQuery.cpp`, `ClientBase.cpp`).
- Rust crate build targets in ninja have the form `rust/workspace/cargo-build__ch_rust_<name>`, not just `_ch_rust_<name>`.

## INSERT ... VALUES via --query flag hangs

`INSERT INTO ... VALUES (...)` passed via the `-q`/`--query` flag to `clickhouse client` hangs indefinitely in the native TCP protocol when `async_insert` defaults to `1` (which is the case in recent ClickHouse builds). This is a pre-existing protocol behavior, NOT a bug in the code under test and NOT a sign that the debug build is too slow.

**Fix for shell tests**: Use stdin piping: `echo "INSERT INTO t VALUES (1)" | $CLICKHOUSE_CLIENT` instead of `$CLICKHOUSE_CLIENT --query "INSERT INTO t VALUES (1)"`. Piped INSERTs and INSERTs inside multiquery blocks from stdin (`--multiquery < file.sql`) work fine.

**Do not** blame the debug build or try switching to a release build when INSERTs hang — the test itself needs fixing.

## `getSubcolumnNameForStream(path, true)` is not `prefix_len = 1`

The second parameter is `encode_sparse_stream`, not `prefix_len`. The `prefix_len` overload takes `size_t`. Do not misread cache-key logic in `ISerialization` helpers.

## `IDataType::getSerialization` needs `SerializationInfoSettings` for correct stream enumeration

`IDataType::getSerialization(column)` without settings defaults String to `SINGLE_STREAM`, missing the `StringSizes` substream. Always pass `SerializationInfoSettings` from the part's `serialization.json` (via `getSerializationInfos().getSettings()`) when enumerating streams for checksums/file-existence checks. The reader helper `getSerializationForPhysicalColumn` already does this correctly.

## Flattened Nested siblings with physical names: shared offset stream + separate caches

With physical names, flattened Nested subcolumns (e.g. `n.x`, `n.y`) use separate `SubstreamsCache` instances (`caches["n.x"]` vs `caches["n.y"]`) but share a single on-disk offset stream (`1.size0.bin`). Without physical names they are subcolumns of `"n"` and share `caches["n"]`, so offsets cached by the first sibling are reused. On S3, prefetching marks the shared stream as consumed → second sibling skips `seekToMark` → reads garbage. Fix: after reading each sibling, pre-populate upcoming siblings' caches with the offset data via `addColumnWithNumReadRowsToSubstreamsCache`.

## Single-child flattened `Nested` can get plain counter physical names

When physical names are allocated for flattened `Nested`, only parents with multiple newly added children go through the compound `{counter}.child` path. A single-child `Nested(x T)` falls through the plain-column allocator and can legitimately get a physical name like `5` (no dot). Do not assume flattened `Nested` always has dotted physical names when reasoning about rename or offset-stream crash safety.

## `clickhouse-test` script may hang on "Connecting to ClickHouse server"

When running the test harness against a custom port, the script can hang during connection if the server is slow or there's a mismatch. Running tests directly with `clickhouse client --multiquery < test.sql` is a reliable alternative for local verification.
