# Integration test artifacts

The `logs.tar.gz` bundle (listed under `=== Artifact Links ===` when using `--links`) is the
primary source. Download it with `--download-logs <path>` or `curl`.

> **Note: compression depends on file size.**  The bundle itself is always a tar archive, but
> individual members inside it may vary. More importantly, the bundle URL may be
> `logs.tar.gz` or `logs.tar.zst` depending on size — check the actual URL from `--links`
> and use `tar -xf` (auto-detects format) rather than `tar -xzf`.

## Archive layout

```
ci/tmp/
  pytest_parallel.jsonl        # structured per-test results (one JSON object per line)
  pytest_parallel.log          # human-readable combined output
  pytest_parallel-gw0.log      # per-worker output: full DEBUG log of docker/cluster calls
  pytest_parallel-gw1.log
  ...
  parallel.log                 # top-level parallel runner log
  job.log                      # CI job script execution log
  docker-in-docker.log         # Docker daemon output

tests/integration/<test_module>/
  _instances-conftest.py-gw<N>/
    docker.log                 # docker compose up/down output for this worker
    node1/logs/
      clickhouse-server.log    # ClickHouse server log for node1
      clickhouse-server.err.log
    node2/logs/
      clickhouse-server.log
      ...
```

## Key files by failure type

**Assertion / wrong result** (`AssertionError`, wrong row count, etc.)

`pytest_parallel.jsonl` — query by `nodeid`:

```bash
grep -F '<test_name>' "tmp/investigate/$SHA/ci/tmp/pytest_parallel.jsonl" \
  | jq 'select(.outcome == "failed") | {
      crash:    .longrepr.reprcrash.message,
      lines:    [.longrepr.reprtraceback.reprentries[].data.lines[]?],
      stderr:   (.sections[] | select(.[0] == "Captured stderr call") | .[1])?,
      captured: (.sections[] | select(.[0] == "Captured log call")    | .[1])?
    }'
```

`longrepr` is a JSON object, not a string. The assertion text is in
`.longrepr.reprcrash.message`. Captured pytest output (docker calls, ClickHouse queries,
Spark protocol) lives in `.sections[]` entries with headers `"Captured stderr call"` and
`"Captured log call"`.

**Server-side exception / wrong data**

`tests/integration/<test_module>/_instances-conftest.py-gw<N>/node<M>/logs/clickhouse-server.log`
(plain text, inside the archive). Extract and grep:

```bash
tar -xf "tmp/investigate/$SHA/logs.tar.gz" -C "tmp/investigate/$SHA/" \
  'tests/integration/<test_module>/_instances-conftest.py-gw1/node1/logs/clickhouse-server.log'
grep -i 'error\|exception\|fatal' \
  "tmp/investigate/$SHA/tests/integration/<test_module>/_instances-conftest.py-gw1/node1/logs/clickhouse-server.log" \
  | tail -50
```

**Docker / container startup failure**

`_instances-conftest.py-gw<N>/docker.log` — docker compose output for that worker.

**Verbose test trace** (which queries ran, in what order)

`ci/tmp/pytest_parallel-gw<N>.log` for the worker that ran the test (find with `grep -l '<test_name>' tmp/investigate/$SHA/ci/tmp/pytest_parallel-gw*.log`).

## Fetching individual members without extracting everything

```bash
# Extract only the JSONL and one node's server log
tar -xf "tmp/investigate/$SHA/logs.tar.gz" -C "tmp/investigate/$SHA/" \
  ci/tmp/pytest_parallel.jsonl \
  'tests/integration/<test_module>/_instances-conftest.py-gw1/node1/logs/clickhouse-server.log'
```
