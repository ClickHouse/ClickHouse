# Local ClickHouse performance testing reference

Use local testing when:

- the user wants to validate a patch before/after CI;
- performance.ci is ambiguous and a controlled local pair can help;
- the user is writing or modifying `tests/performance/*.xml`;
- CI artifacts are unavailable.

Local results are not automatically CI-equivalent. Always report machine, build type, settings, dataset, server ports, run count, and whether the result repeats.

## Requirements

Prefer:

- release or release-like static builds;
- same compiler/build options for old and new binaries;
- isolated server data dirs;
- same ClickHouse config except ports/paths;
- same datasets and settings;
- idle machine.

Avoid unless explicitly requested:

- debug builds;
- sanitizer builds;
- comparing a local build against a public CI binary without noting toolchain bias;
- reusing dirty server data dirs.

## Binary sourcing

Local comparison requires two executable `clickhouse` binary paths:

- `OLD_CLICKHOUSE`: old/base/reference binary.
- `NEW_CLICKHOUSE`: new/candidate binary.

The helper does **not** download or build ClickHouse:

```bash
mkdir -p tmp
scripts/local_servers.sh /path/to/old/clickhouse /path/to/new/clickhouse tmp/ch-perf-local
```

If either path is missing, ask the user/caller for it before starting servers. Do not silently guess a binary, build from source, or download artifacts without confirmation.

Record, at minimum:

- old and new binary paths;
- `clickhouse local -q "SELECT version(), buildId()"` or server `SELECT version(), buildId()` output for each side;
- source SHA/build URL if known;
- architecture and build type.

### Suggesting Praktika CI downloads

If the user does not already have binaries, suggest exact Praktika CI artifacts and ask whether to download them. Derive the inputs from the performance.ci run identity:

```bash
python3 scripts/perf_api.py runs --pr "$PR"
# Use identity.newSha for the candidate, identity.oldSha for the reference/base,
# and the run's arch (`amd` or `arm`) for ARCH.
```

Performance comparison jobs normally use release artifacts:

- AMD: `build_amd_release`
- ARM: `build_arm_release`

For a PR candidate build:

```bash
PR=<pr-number>
SHA=<candidate-sha>
ARCH=arm        # arm or amd
JOB="build_${ARCH}_release"
REPORT="https://clickhouse-builds.s3.amazonaws.com/PRs/${PR}/${SHA}/${JOB}/artifact_report_${JOB}.json"

curl -fsS "$REPORT" | jq -r '.build_urls[] | select(endswith("/clickhouse"))'
```

For an exact master/reference build:

```bash
SHA=<reference-sha>
ARCH=arm        # arm or amd
JOB="build_${ARCH}_release"
URL="https://clickhouse-builds.s3.amazonaws.com/REFs/master/${SHA}/${JOB}/clickhouse"

curl -sfI "$URL"
```

If the URL exists and the user approves:

```bash
mkdir -p tmp/ch-binaries
curl -fL "$URL" -o "tmp/ch-binaries/clickhouse-${SHA}-${ARCH}"
chmod +x "tmp/ch-binaries/clickhouse-${SHA}-${ARCH}"
```

For PR artifacts, prefer the URL returned by `artifact_report_${JOB}.json`; for master/reference artifacts, verify the exact `REFs/<branch>/<sha>/<job>/clickhouse` URL with `curl -sfI`. Avoid non-exact `master/amd64/clickhouse` or `master/aarch64/clickhouse` fallbacks for validation unless the user explicitly accepts that they are not SHA-pinned.

## Starting servers

If the user already has two servers, use them.

If not, this skill includes an optional helper:

```bash
mkdir -p tmp
scripts/local_servers.sh /path/to/old/clickhouse /path/to/new/clickhouse tmp/ch-perf-local
```

It starts by default:

- old/base: TCP `9000`, HTTP `8123`
- new/candidate: TCP `9001`, HTTP `8124`

Override ports if defaults are busy:

```bash
mkdir -p tmp
OLD_TCP_PORT=9100 NEW_TCP_PORT=9101 OLD_HTTP_PORT=8223 NEW_HTTP_PORT=8224 \
  scripts/local_servers.sh /path/to/old/clickhouse /path/to/new/clickhouse tmp/ch-perf-local
```

Verify:

```bash
clickhouse-client --port ${OLD_TCP_PORT:-9000} -q "SELECT version(), buildId()"
clickhouse-client --port ${NEW_TCP_PORT:-9001} -q "SELECT version(), buildId()"
```

Stop using the printed PID files or:

```bash
kill "$(cat tmp/ch-perf-local/old/clickhouse.pid)" "$(cat tmp/ch-perf-local/new/clickhouse.pid)"
```

The helper is intentionally minimal. If a test requires special config, use an existing project config instead.

## Dataset setup

Some performance tests create/fill their own tables via XML `create_query` and `fill_query`.

For shared datasets such as hits, visits, TPC-H, TPC-DS, use the repository scripts and verify row counts. Do not overwrite existing user data silently.

Useful checks:

```bash
clickhouse-client --port ${OLD_TCP_PORT:-9000} -q "SHOW DATABASES"
clickhouse-client --port ${NEW_TCP_PORT:-9001} -q "SHOW DATABASES"
```

If using existing tables:

```bash
--use-existing-tables
```

Only use this when both servers have identical datasets.

## Inspect a performance test

Print expanded query texts:

```bash
tests/performance/scripts/perf.py --print-queries tests/performance/$TEST.xml
```

Print ClickHouse settings from the XML:

```bash
tests/performance/scripts/perf.py --print-settings tests/performance/$TEST.xml
```

## Run one test

```bash
mkdir -p tmp
tests/performance/scripts/perf.py \
  --host 127.0.0.1 127.0.0.1 \
  --port ${OLD_TCP_PORT:-9000} ${NEW_TCP_PORT:-9001} \
  --runs 7 \
  tests/performance/$TEST.xml | tee tmp/${TEST}.perf.tsv
```

Run one query index:

```bash
mkdir -p tmp
tests/performance/scripts/perf.py \
  --host 127.0.0.1 127.0.0.1 \
  --port ${OLD_TCP_PORT:-9000} ${NEW_TCP_PORT:-9001} \
  --runs 7 \
  --queries-to-run "$QUERY_INDEX" \
  tests/performance/$TEST.xml | tee tmp/${TEST}_${QUERY_INDEX}.perf.tsv
```

Allow long tests only when expected:

```bash
--long
```

Use profiling only after a meaningful difference appears:

```bash
--profile-seconds 30
```

Profiling can perturb measurements; do not mix profiled timings with normal timings.

## Parse perf.py output

```bash
python3 scripts/parse_perf_py.py tmp/${TEST}_${QUERY_INDEX}.perf.tsv
```

Important output lines:

```text
report-threshold <relative_threshold>
query <query_index> <run_id> <server_index> <elapsed>
client-time <query_index> <client_seconds> <server_seconds>
median <query_index> <median_server_0>
diff <query_index> <old_median> <new_median> <relative_diff> <pvalue>
```

`relative_diff` is `(new_median - old_median) / old_median`:

- positive: new/candidate slower;
- negative: new/candidate faster.

Meaningful local signal:

- use at least `--runs 3`; prefer `--runs 7` or more for noisy queries;
- `abs(relative_diff) >= report-threshold`;
- `pvalue <= 0.05`;
- repeat the local comparison before calling a regression real, especially if local data is the only evidence;
- server self-comparison or rerun is stable if the result is surprising.

Do not use a single local run as a standalone verdict. If the result has not repeated, report it as `needs rerun` or `local-only evidence`, not a real regression.

## Optional trace analysis on local servers

If a query ID is known, inspect `system.trace_log`:

```sql
SELECT
    count() AS samples,
    demangle(addressToSymbol(trace[1])) AS function
FROM system.trace_log
WHERE query_id = '<query_id>'
  AND trace_type = 'CPU'
GROUP BY function
ORDER BY samples DESC
LIMIT 30
SETTINGS allow_introspection_functions = 1
```

Collapsed stacks:

```sql
SELECT concat(
    arrayStringConcat(arrayReverse(arrayMap(x -> demangle(addressToSymbol(x)), trace)), ';'),
    ' ',
    toString(count())
)
FROM system.trace_log
WHERE query_id = '<query_id>'
  AND trace_type = 'CPU'
GROUP BY trace
FORMAT TSVRaw
```

Compare `CPU` and `Real`:

- CPU up with runtime up: compute regression likely.
- Real up with CPU flat: wait, lock, I/O, scheduler, or background work likely.
- Memory up with time up: allocation pressure or changed algorithm likely.
