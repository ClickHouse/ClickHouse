# Stateless / Fast test artifacts

All artifacts are individual files. List their URLs with `--links`, then fetch what you need.

> **Note: compression depends on file size.**  Praktika uploads small files as plain text and
> large ones as zstd-compressed `.zst` objects. The URL shown by `--links` reflects the actual
> S3 key, so use it as-is. When constructing a URL manually, try the plain name first; if you
> get a 404, append `.zst` and decompress. Never assume a fixed extension for a given file.

## File catalog

| File | Contains |
| --- | --- |
| `clickhouse-server.log[.zst]` | Main ClickHouse server log |
| `clickhouse-server.err.log[.zst]` | Server stderr (startup errors, fatal signals) |
| `clickhouse-local.log[.zst]` | clickhouse-local output (Fast test only) |
| `clickhouse-local.err.log[.zst]` | clickhouse-local stderr (Fast test only) |
| `stderr.log[.zst]` | Test runner stderr |
| `job.log[.zst]` | CI job script execution log |
| `coordination.tar.gz` | Keeper coordination logs (Fast test only) |

## Fetching

Always check the URL extension from `--links` first:

```bash
# Plain text file
curl -sL '<url>' | tail -200

# Zstd-compressed file (.zst suffix in the URL)
curl -sL '<url>' | zstd -dcq | grep -i 'error\|exception\|fatal' | tail -50

# Unknown — download first, then check
curl -sL '<url>' -o "tmp/investigate/$SHA/artifact"
file "tmp/investigate/$SHA/artifact"   # shows "Zstandard compressed data" or "ASCII text", etc.
# then: zstd -dcq "tmp/investigate/$SHA/artifact" | ...   or   cat "tmp/investigate/$SHA/artifact" | ...
```

## Key files by failure type

**Reason already in `result.info`** (most stateless failures)

The tool already shows the failure reason: `Reason: <type>:` followed by the actual output
or diff. Go to the artifacts only when the reason section is truncated or the server-side
context is needed.

**Test output diff** (`Reason: result differs`)

The diff is in `result.info`. For the full test output: `stderr.log` (test runner output
includes actual vs expected).

**Server exception / crash** (`Reason: having exception in stdout`, `Server died`)

`clickhouse-server.err.log.zst` — contains the full exception with stack trace.
`clickhouse-server.log.zst` — search around the timestamp from `result.info`.

**Test runner infra failure** (timeout, docker issue)

`job.log` — the raw CI job script output, shows what ran and where it stopped.
