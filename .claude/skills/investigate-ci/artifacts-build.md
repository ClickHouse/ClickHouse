# Build artifacts

> **Note: compression depends on file size.**  Praktika uploads small files as plain text and
> large ones as zstd-compressed `.zst` objects. The URL shown by `--links` reflects the actual
> S3 key, so use it as-is. When constructing a URL manually, try the plain name first; if you
> get a 404, append `.zst` and decompress.

## What the tool already shows

`fetch_ci_report.js --failed` extracts a head+tail window from `result.info`:

- **Head**: the beginning of the log (or, for large logs, the point after the CI harness
  truncation marker `~~~~~ truncated N lines at the beginning ~~~~~`)
- **Tail**: the end of the log, where `ninja: build stopped: subcommand failed` and the
  compiler errors (`error:`, `note:`) appear

For most compile errors this window is sufficient. Go to the full log only when the
relevant error is outside the visible window.

## Full build log

The linked `build_clickhouse.log` (or `build_clickhouse.log.zst`) file is listed under
`=== Artifact Links ===` with `--links`. It may be large (hundreds of MB for full builds).
Check the URL extension to know whether to decompress.

```bash
# Plain log — stream and grep for errors
curl -sL '<url>' | grep -E '^[^ ].*(error|FAILED):' | head -50

# Compressed log
curl -sL '<url>' | zstd -dcq | grep -E '^[^ ].*(error|FAILED):' | head -50

# For clang-tidy (plain): filter out suppressed-warning noise
curl -sL '<url>' | grep -v 'Suppressed\|NOLINT\|warnings generated' \
  | grep -i 'error:' | head -50

# For clang-tidy (compressed):
curl -sL '<url>' | zstd -dcq | grep -v 'Suppressed\|NOLINT\|warnings generated' \
  | grep -i 'error:' | head -50
```

## Log truncation

When `result.info` has both a beginning and end truncation marker
(`~~~~~ truncated N lines at the beginning ~~~~~` at the top,
`~~~~~ truncated N lines at the end ~~~~~` at the bottom), the actual errors are in the
truncated section. Fetch and search the full `build_clickhouse.log` directly.

## Key patterns by build type

**Regular compiler error** (`amd_binary`, `arm_debug`, etc.)

Errors appear near the end of the log — look for lines matching `error:` followed by
`ninja: build stopped`.

**clang-tidy** (`arm_tidy`, `amd_tidy`)

Each file emits a warning count summary. Actual tidy errors are lines like
`/path/to/file.cpp:NN:MM: error: [clang-tidy-check-name]`. They appear mid-log,
interleaved with the build progress. Grep for `error:` while suppressing the
`Suppressed N warnings` noise lines.

**Link error**

`undefined reference` or `cannot find -l<lib>` near the very end, just before
`ninja: build stopped`.
