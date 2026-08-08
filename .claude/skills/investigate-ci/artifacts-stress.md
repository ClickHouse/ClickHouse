# Stress test artifacts

All artifacts are individual files. List their URLs with `--links`, then fetch what you need.

> **Note: compression depends on file size.**  Praktika uploads small files as plain text and
> large ones as zstd-compressed `.zst` objects. The URL shown by `--links` reflects the actual
> S3 key, so use it as-is. When constructing a URL manually, try the plain name first; if you
> get a 404, append `.zst` and decompress. Never assume a fixed extension for a given file.

## File catalog

| File | Contains |
| --- | --- |
| `clickhouse-server.initial.log[.zst]` | Server log at first startup (key for "Cannot start") |
| `clickhouse-server.log[.zst]` | Full server log during the entire run |
| `clickhouse-server.err.log[.zst]` | Server stderr |
| `clickhouse-server.stress.log[.zst]` | Server log during the stress phase only |
| `fatal.log[.zst]` | Fatal signal / abort output |
| `hung_check.log[.zst]` | Deadlock / hung check output |
| `stderr.log[.zst]` | Test runner stderr |
| `stdout.log[.zst]` | Test runner stdout |
| `test_results.tsv` | Machine-readable test results (TSV) |
| `stress_run_logs.tar.zst` | Bundle of all per-run logs |
| `job.log[.zst]` | CI job script execution log |

## Key files by failure type

**`Cannot start clickhouse-server`**

`clickhouse-server.initial.log` — plain text, small, shows exactly why the server failed
to start (metadata load errors, table load failures, config issues).

**Logical error / assertion abort** (STID-tagged failures)

The full exception and stack trace are already in `result.info`. Cross-check the server
log around the time of the abort:

```bash
curl -sL '<clickhouse-server.stress.log.zst url>' | zstd -dcq \
  | grep -i 'logical error\|assertion\|fatal\|STID' | head -30
```

**Crash / signal** (`SIGSEGV`, `SIGABRT`)

`fatal.log` — written by the watchdog, contains the signal and initial stack.
`clickhouse-server.err.log.zst` — full symbolized trace.

**Deadlock / hung query**

`hung_check.log` — output of the hung-check script; lists what was running at detection
time.

**Fetching**

Always check the URL extension from `--links` first:

```bash
# Plain text file
curl -sL '<url>' | tail -200

# Zstd-compressed file (.zst suffix in the URL)
curl -sL '<url>' | zstd -dcq | grep -i 'error' | tail -50

# Unknown — download first, then check
curl -sL '<url>' -o "tmp/investigate/$SHA/artifact"
file "tmp/investigate/$SHA/artifact"   # shows "Zstandard compressed data" or "ASCII text", etc.
# then: zstd -dcq "tmp/investigate/$SHA/artifact" | ...   or   cat "tmp/investigate/$SHA/artifact" | ...
```
