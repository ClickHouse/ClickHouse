## ClickHouse performance tests

This directory contains `.xml`-files with performance tests.

### Which tests run in pull requests

Ordinary PR comparisons against master choose whole XML tests from a recent
performance coverage export. They use the shared stateless and integration
selection rules independently, with limits of 250 and 100 tests, then combine
their results without another limit. Only query execution coverage is used:
setup and teardown still run, but their coverage does not choose tests. Regions
covered by more than 300 tests are ignored. A changed source header also adds
its covering tests when at most 300 tests cover that header.

The selection always includes the fixed set in
[`performance_fixed_tests.json`](../../ci/jobs/scripts/performance_fixed_tests.json),
tests that failed earlier in the PR, and new or changed tests. Failure history
uses the preceding 30 days of normal performance runs: slowdowns in failed jobs,
recorded dashboard failures and XML execution errors. As in the shared selectors,
a commit and job with failures in 20 or more tests is excluded from this history.
Changes to included XML, query files and settings count as changes to their owning test. Tests that
have no coverage, including shell tests, are included too. Every query in every
selected XML runs, with the usual adaptive timing repetitions. These runs use six
shards per enabled architecture.

The `pr-performance` label runs the full test suite on eight ARM shards; AMD
uses coverage selection on six shards. Changes that coverage cannot see use the
same rule and enable both architectures: submodules, build files, compiler images,
the performance harness, fixtures and server configuration, setting defaults,
and the constants in `src/Core/Defines.h` used by default expressions.
The `ci-force-all` label runs the full suite on eight shards per architecture.
Full runs retain the existing query sampling settings. Master, release, nightly
and special Iceberg comparisons keep their existing behavior.

Each job attaches `performance_selection.json` with its tests, selection reasons,
coverage identities, source mapping gaps and query policy. Tests whose coverage
failed or was not collected are always selected. A transient CIDB or
GitHub failure skips the job with a warning and is not cached; rerun it to test.
Selection reduces work, but does not guarantee that every performance change is
caught. Coverage collected on AMD is also used for ARM selection.

### How to write a performance test

First of all please check that existing tests don't cover your case. If there are no such tests then you can write your own test.

Test template:

``` xml
<test>
    <!-- Optional: Specify settings -->
    <settings>
        <max_threads>1</max_threads>
        <max_insert_threads>1</max_insert_threads>
    </settings>

    <!-- Optional: Variable substitutions, can be referenced to by curly brackets {} and used in any queries -->
    <substitutions>
        <substitution>
            <name>x</name>
            <values>
                <value>10</value>
                <value>50</value>
            </values>
        </substitution>
        <substitution>
            <name>y</name>
            <values>
                <value>5</value>
                <value>8</value>
            </values>
        </substitution>
    </substitutions>

    <!-- Optional: Table setup queries -->
    <create_query>CREATE TABLE tab1 [..]</create_query>
    <create_query>CREATE TABLE tab2 [..]</create_query>

    <!-- Optional: Table population queries -->
    <fill_query>INSERT INTO tab1 [...]</fill_query>
    <fill_query>INSERT INTO tab2 [...]</fill_query>

    <!-- Benchmark queries -->
    <query>SELECT [...] WHERE col BETWEEN {x} AND {y}</query>
    <query>SELECT [...]</query>
    <query>SELECT [...]</query>

    <!-- Optional: Table teardown queries -->
    <drop_query>DROP TABLE tab1</drop_query>
    <drop_query>DROP TABLE tab2</drop_query>
</test>
```

The drop queries also run when the test fails, and a test whose drop queries cannot be shown to have cleaned up stops the rest of its batch.

If your test takes more than 10 minutes, please, add tag `long` to have an opportunity to run all tests and skip long ones.

### Queries whose time keeps growing with each run

Every query runs several times on the same tables, with nothing reset between the runs. The check of a pull request that adds or changes a test fails when the time of a query of the test keeps growing with each run on the tested server (at least 5 runs, correlation of run number and time above 0.8, last run over first above 1.2). Usually each run reads what the previous runs changed, for example a table that grows with every timed `INSERT`. A pull request job runs at most 10 queries of a test, chosen at random, unless the test sets `run_all_queries="1"`. Make every run do the same work, for example write to `/dev/null` instead of a file, or make an `UPDATE` skip the rows it already rewrote.

### Disabling a query

The performance history of a query is keyed by the test name and the query number (`query_index`, 0-based, after substitutions), so removing or reordering a query moves the history of every later query of the test. To stop running a query without that, mark it with `disabled="1"`:

```xml
<query disabled="1">SELECT [...]</query>
```

A disabled query keeps its number, and `perf.py --print-queries` still prints it, but it is never run, and the setup statements of a disabled `<query file=...>` are not run either. On a query with substitutions, the attribute applies to all of its expansions.

### Setup queries that cannot run on the reference server

A `create_query`/`fill_query` normally must succeed on both servers being compared. If your PR adds a feature the setup depends on (e.g. a new MergeTree setting), the query cannot work on the reference (master) server yet, and the test would fail the CI check. Opt out of the reference-side check with `do_not_check_in_pr="<number of your PR>"`:

```xml
<create_query do_not_check_in_pr="12345">CREATE TABLE tab (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS new_setting = 1</create_query>
```

When such a query fails on the reference server, the remaining setup queries are skipped there, the whole test runs on the new server only, and its queries are reported under "Backward-incompatible queries" instead of failing the check. The opt-out applies only in the PR named by the attribute. The attribute is only allowed on top-level `create_query`/`fill_query` elements and is not compatible with `<query type="shell">` in the same test.

### Shell-script queries

In addition to SQL queries sent over the native protocol, a benchmark query can be a shell script, marked with `type="shell"`. This is useful for end-to-end measurements that the native protocol cannot express: HTTP latency, response compression, tool startup time, etc.

``` xml
<test>
    <!-- How fast clickhouse-local starts up. -->
    <query type="shell"><![CDATA[
        $CLICKHOUSE_LOCAL --query "SELECT 1" > /dev/null
    ]]></query>

    <!-- Reading ~1 MB of gzip-compressed data over HTTP. -->
    <query type="shell"><![CDATA[
        ${CLICKHOUSE_CURL} -H 'Accept-Encoding: gzip' \
            "${CLICKHOUSE_URL}?enable_http_compression=1" \
            --data-binary "SELECT number FROM numbers(500000) FORMAT TSV" \
            -o /dev/null
    ]]></query>
</test>
```

Each shell-script query is run with `bash -e -o pipefail` once per server (the reference build and the patched build), and timed by its wall-clock time, the same way a SQL query's time becomes the `client_time` metric in the report. Wrap the script in `<![CDATA[ ... ]]>` so that `<`, `>` and `&` do not need XML escaping. A non-zero exit code is treated as a failed query.

The script talks to the server using environment variables that mirror the stateless tests in `tests/queries/shell_config.sh`, prepared per-server so that the reference and the patched build (which listen on different ports) are measured each on their own:

* `CLICKHOUSE_BINARY` — path to the `clickhouse` binary,
* `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT_TCP`, `CLICKHOUSE_PORT_HTTP` — server endpoints,
* `CLICKHOUSE_CLIENT`, `CLICKHOUSE_LOCAL` — ready-to-run client and local commands,
* `CLICKHOUSE_CURL`, `CLICKHOUSE_URL` — `curl` invocation and the HTTP URL,
* `CLICKHOUSE_DATABASE` — the database name (`default`).

Notes:

* Parameter `{substitutions}` are **not** applied to shell scripts, because they use `${var}` and `{a,b}` brace expansion that would collide with the substitution syntax. Use shell loops or environment variables instead.
* The `<settings>` element is **not** applied to shell scripts; pass settings through the URL or client arguments inside the script.
* Profiler runs and server-side `ProfileEvents` are not collected for shell scripts (there is no single query to attribute them to); only the timing difference is reported.
* `CLICKHOUSE_CURL` is `curl -q -sS --fail --max-time 120`. Unlike `tests/queries/shell_config.sh`, it adds `--fail` so that an HTTP 4xx/5xx response makes `curl` exit non-zero and the query fails — a benchmark must never time a server error response as a fast successful sample.
* On timeout (`--max-query-seconds`, `--prewarm-max-query-seconds`) the whole process group of the script is killed, not just the immediate `bash`, so a script blocked inside a child such as `curl` or `$CLICKHOUSE_LOCAL` cannot keep running and pollute later measurements.

### Long tests (nightly only)

Tests tagged `long` are skipped in PR and master runs (`perf.py` skips them unless given `--long`). A nightly workflow runs them and labels its rows in the performance tables `workflow_name = 'NightlyExtendedPerformance'`; its schedule lives in that workflow (`ci/workflows/nightly_extended_performance.py`; rerun with `gh workflow run NightlyExtendedPerformance`). Examples: `calibration.xml` (the per-host yardstick), `tpch_sf100.xml`, `tpcds_sf100.xml`.

How to write one:

* `<test run_all_queries="1" max_query_seconds="N">`: `max_query_seconds` replaces `--max-query-seconds` and `--prewarm-max-query-seconds` for this test. The budget applies to each statement of a query; a query stops being repeated once its cumulative time per server reaches three budgets.
* Put `<max_execution_time>0</max_execution_time>` into `<settings>`, otherwise long setup statements are cut by the server profile limit. Prewarm and timed runs still get the budget.
* Load raw data with `fill_query` and finish every loaded MergeTree table as `tpch_sf100.xml` does: insert; `MODIFY SETTING lock_acquire_timeout_for_background_operations`; `OPTIMIZE ... FINAL` with `optimize_throw_if_noop = 1`; row and part counts; then content checks. `tpch_sf100.xml` also runs `MATERIALIZE STATISTICS` (`mutations_sync = 1`) before `SYSTEM STOP MERGES`, which blocks mutations too; for a test whose queries only read, the harness stops merges after the setup (`perf.py --stop-merges`).
* Keep the tables in `default` (no `CREATE DATABASE`, no `USE`) and drop them with `DROP TABLE IF EXISTS <t> SYNC`, so the disk is free for the next test. (`USE` in existing tests is reconnect-safe: `clickhouse_driver` re-sends the database selected by `USE` when it reconnects.)
* Job-local S3 (`<test requires_s3="1">`): write through the `perf_s3` named collection (each server has its own prefix, emptied after each such test in the nightly; a failed cleanup is an error and ends the batch); read the shared immutable data through `perf_s3_data` (`requires_s3_read_dataset="1"`: `tpch_ice10_s3`; `requires_s3_public_data="1"`: the prefixes in `s3_service.PUBLIC_PREFIXES` under their public keys). `iceberg_delete_data_on_drop` in `<settings>` has no effect: a drop uses the server's default profile.
* Arrow Flight: the nightly servers listen on `arrowflight_port`; address the server itself as `arrowFlight('127.0.0.1:' || toString(getServerPort('arrowflight_port')), '<table in default>')`, the same text on both servers.
* Iceberg REST catalog (`requires_s3_catalog="1"`): the job-local SeaweedFS serves it on `localhost:8181`; the harness seeds the immutable table bucket `perf-catalog` (namespace `many`, tables `t1`..`t200`, one row each, `id = n`). Such a test creates one `DataLakeCatalog` database as its only exception to the `default`-only rule and drops it with `DROP DATABASE ... SYNC`; the fixed ports are part of the test text.
* Every timed statement with side effects (`ALTER`, `INSERT`, `OPTIMIZE`, `DELETE`, table recreation) is a shell query: `perf.py` profiles a query whose timing changed (every query with `profile_all_queries`) right after its runs, each statement cut at 10 s, and a cut mutation keeps running into the next query; shell queries are not profiled. A shell query gets its settings on the client command line (`--mutations_sync 2`, `--allow_insert_into_iceberg 1`, ...); the test's `<settings>` do not apply to it.
* Reuse `tests/benchmarks/` with `<query file="../benchmarks/<name>/queries/query_NN.sql"/>` and `<settings file="../benchmarks/<name>/settings.json"/>`.
* Skip a known-problem query with `disabled="1"` (see [Disabling a query](#disabling-a-query)) or keep a working query in its place (a fixed formulation, or a placeholder such as `SELECT 'Q35 skipped'`), and describe the original query, the reason and the issue link in the benchmark's README (e.g. `tests/benchmarks/tpc-ds/README.md`). A flaky query gets an issue and is removed.

Run one test locally:

```
# Against two running servers.
tests/performance/scripts/perf.py --long --host localhost localhost --port 9001 9002 --binary <old> <new> --http-port 8123 8124 tests/performance/tpch_sf100.xml
# The whole harness as the nightly job runs it; the tested binary is ./ci/tmp/clickhouse or --ch-path <dir>.
python3 ci/jobs/performance_tests.py --test-options "arm_release, master_head, nightly, 1/1" --reference-path <older clickhouse> --test tpch_sf100
```

With the `nightly` test option the harness sorts the test list, slices it round-robin over the `N/M` batches except the tests in `NIGHTLY_PERF_PLACEMENT` (`ci/defs/defs.py`; `"<test>.xml"` maps to `"N/4"`, a batch; to an extra job name matching `[a-z0-9_]+`, a job of its own; or to `"skip"`, run nowhere: the kill switch, effective the next night), prepends `calibration.xml` to every job, runs all queries with `perf.py --long`, and skips the performance dashboard and delta gates.

Red and green. A timeout is `TIMEOUT_EXCEEDED` (for a shell query, its time limit); anything else is an error. The tested side is the new server, the reference side the old one.

| | tested side, alone or with the reference | reference side only |
|---|---|---|
| prewarm error or timeout | red (`run-error` line) | `partial`, measured on the tested side |
| timed-run timeout | red | with `--long`: censored row, both sides keep running; otherwise red |
| timed-run error | red | red |

A censored row records the time of the completed statements plus the budget. With `--long`, regressions and `partial` queries are visible but do not turn the job red, and:

* A settings or setup failure on the reference side (a feature the older binary lacks) runs the test on the tested side only, reported as `partial`, without `do_not_check_in_pr`. A setup failure on the tested side is red.
* Every test runs under `timeout -k 60` with a 4-hour budget. A non-zero exit always leaves an error record (`124`: budget expired, `137`: killed), so the job is red. A failed upload of the required results (raw runs, query metrics, test times) is red too.
* Teardown: on exit, including `SIGTERM`, `perf.py` runs the drop queries on every server that ran setup and prints `teardown-complete` only if all of them succeeded (a missing table is accepted on a reference whose setup failed). Without that line the harness records `teardown incomplete, stopping the batch` and runs no further tests on that host; what was measured is still reported.
* Changes reported by the nightly are unconfirmed: `confirm_changes` is skipped, and the repetition from night to night is the confirmation.

To get the text of a reported `query_index` (0-based, after substitutions), check out the tests of that run and use `tests/performance/scripts/perf.py --print-queries tests/performance/<test>.xml --queries-to-run <query_index>`.

#### Conditions changelog

Nightly results are comparable only under the same conditions. Append a dated line whenever any of them changes:

* YYYY-MM-DD (first scheduled night): instance type `r8g.4xlarge`, image `clickhouse/performance-comparison`, pool `arm-medium-storage`.
* YYYY-MM-DD (first night with the new tests): 26 new `long` tests (Iceberg and data lakes, formats, Arrow Flight, vector index, JSON, engine, projections, skip indexes, analyzer, query shapes, encodings), `NIGHTLY_PERF_PLACEMENT` pins, Arrow Flight port and ticket flags on the nightly servers, S3 public-data and REST-catalog seeding, S3 cleanup after every S3 test.
* YYYY-MM-DD (first night with the test): `iceberg_suite_local_tpcds_sf100.xml` (TPC-DS SF100 over `IcebergLocal` tables) in `2/4`.

Each job log also carries the resolved docker image digest (printed by the praktika runner) and the host identity (kernel, OS, CPU, memory, instance).

### Iceberg configuration

The same tests also run with their MergeTree and `Memory` tables created as Iceberg tables, so that their queries measure the Iceberg read and write path: `perf.py --table-engine iceberg_local` (`IcebergLocal` under `user_files`) or `--table-engine iceberg_s3` (`IcebergS3` through the `perf_s3` named collection of the job-local S3 endpoint). `tests/performance/scripts/iceberg_engine.py` formats each `CREATE TABLE` of the setup with `clickhouse format --oneline` and rewrites a table of any MergeTree engine or of `Memory`:

* the engine becomes `IcebergLocal` or `IcebergS3`, with the path `perf_iceberg/<test>/<table>/`; a `CREATE TABLE b AS a` of a rewritten `a` gets its own path;
* the leading plain columns of `ORDER BY` stay as the Iceberg sort order; `PARTITION BY`, `PRIMARY KEY`, `SAMPLE BY`, `TTL`, `SETTINGS`, skip indexes, projections, constraints, codecs and column TTLs are dropped;
* the column types Iceberg cannot store are widened: `UInt8`, `UInt16`, `Int8`, `Int16`, `Bool` to `Int32`, `UInt64` to `Int64`, `LowCardinality(T)` to `T`, `Enum`, `FixedString` and `IPv6` to `String`, `IPv4` to `UInt32`, 128- and 256-bit integers to `Decimal(38, 0)`, `DateTime64` to microseconds. The types of `CREATE TABLE ... AS SELECT` come from the `SELECT` and are not widened.

The behavior of the other MergeTree engines is lost: a `ReplacingMergeTree` or `AggregatingMergeTree` table keeps every inserted row. A lightweight `UPDATE t SET ... WHERE ...` of a rewritten table, in the setup or measured, becomes the mutation `ALTER TABLE t UPDATE ... WHERE ...`: Iceberg tables support mutations but not lightweight updates. Other statements run as written, with `allow_insert_into_iceberg`, `allow_experimental_iceberg_compaction` (for `OPTIMIZE`) and `iceberg_delete_data_on_drop` set. A column that an Iceberg table cannot store even widened (`AggregateFunction`, `JSON`, `Dynamic`, `Variant`, `MATERIALIZED`, `ALIAS`) is an error.

The configuration runs only the tests with a MergeTree or `Memory` table in a `create_query` (a test without one would repeat its MergeTree run; `perf.py --table-engine` skips it, and the nightly does not run `calibration.xml` in this configuration), except the `long` ones, those with `<test iceberg="0">`, and those in `tests/performance/iceberg_skip_list.txt` (one file name per line, with the reason after `#`). Add a test to the skip list when it cannot run on Iceberg tables (`FINAL`, `SAMPLE`, `_part`, assertions on the number of parts, a type that widening breaks); remove it once it can. A failure of a test that is not in the list is a real error of the configuration. A pull request that adds or changes a test the configuration selects runs the Iceberg jobs on that test only, so a new test that cannot run on Iceberg tables shows up in the pull request that adds it: add it to the skip list with the reason, or fix it.

Where it runs: `Performance Comparison (arm_release, master_head, iceberg_local|iceberg_s3, N/3)` in pull requests that change the Iceberg, object storage or Parquet code or the harness (all the selected tests), or a performance test it selects (that test only; the `ci-performance` label does not change this), not as a gate yet, and every night in `NightlyExtendedPerformance` (`ci/workflows/nightly_extended_performance.py`, jobs `arm_release, master_head, nightly, iceberg_local|iceberg_s3, N/2`), every query of every test against the previous night's binary. The results are in the usual tables with `arch = 'arm_iceberg_local'` or `'arm_iceberg_s3'`, so that the history, the learned thresholds and the dashboard gate of the MergeTree runs do not read them; the configuration has no learned thresholds and no dashboard gate.

Run one test locally against two running servers: `tests/performance/scripts/perf.py --table-engine iceberg_local --host localhost localhost --port 9001 9002 --binary <old> <new> --http-port 8123 8124 tests/performance/<test>.xml`.

### Comparison of query results

With `--result-digest`, `perf.py` compares the result of every SQL query between the servers, on its prewarm run, so the measured runs are not affected. It costs one more run of every `FORMAT Null` query per server, so CI does not pass it; use it when running `perf.py` by hand. A query whose result the server sends is digested on the client: the number of rows and the sum of a 64-bit hash of each row, so the order of rows does not matter, with floats rounded to 6 significant digits (parallel aggregation changes the last bits), `Bool` and an integral `Decimal` as integers. A `SELECT ... FORMAT Null`, whose rows the server does not send, is digested on the server by one more query, `SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (<query>)`; a query that cannot be a subquery is listed as `result-digest-skipped`. A result of more than 200000 rows is not compared.

Every digest is a `result-digest` line in the raw output of the test, so digests can be compared across jobs too, e.g. a test in the Iceberg configuration with the same test on MergeTree. When the servers return different results, `perf.py` prints a `result-mismatch` line and the report lists the query in "Queries with Different Results". It is not an error: a pull request that fixes a result changes it. Not compared: the queries of a test whose setup generates data with `rand`, `now` and the like (the servers hold different data), and queries whose own result is not deterministic (`rand`, `any`, `groupArray`, `LIMIT`, `system` tables and the like).

### How to run performance test

TODO

### How to validate single test

```
pip3 install clickhouse_driver scipy
../../tests/performance/scripts/perf.py --min-runs 1 --cap 1 --cap-fast 1 insert_parallel.xml
```

### LLVM coverage collection

`NightlyCoverage` runs 16 separate performance coverage shards using
`amd_llvm_coverage_per_test`. These runs collect source regions; they do not
produce performance measurements or change PR test selection. The regular
performance comparison continues to use release binaries.

The collector attaches the prebuilt dataset parts and renames `datasets.hits_v1`
to `test.hits`, as the PGO collector does. It keeps the fixture rows without the
timing job's bulk rewrite, which is too expensive with LLVM instrumentation.
The manifest records `dataset_layout=prebuilt`; test-defined setup and writes
still run under coverage.

Coverage setup, queries and teardown have no separate statement or socket time
limit. They share the shard's remaining collection budget: twelve hours including
a 20-minute export reserve. This avoids applying release-query limits to heavily
instrumented code. Ordinary performance measurements keep their existing limits.

A failed test remains a job failure, but collection continues after restarting
the server and rerunning that test's teardown. If restart or teardown fails,
or the shard budget expires, the remaining tests are recorded
as not run. Completed tests are still exported. PR selection always includes
every test without completed coverage; a test failure does not invalidate the
coverage of the other tests.

Shard assignment uses measured instrumented runtimes in
`ci/jobs/performance_coverage_timings.json`. Whole XML files are sorted from
longest to shortest, with lexical ties, then placed on the least-loaded shard.
The weights include test setup and teardown, plus measured shard-level fixture
and export overhead. A test whose runtime is only a timeout lower bound can be
marked `isolate_until_measured` in the timing table: it gets a dedicated shard
until a completed run supplies its full wall time. Refreshing that measurement
removes the isolation flag. The lower bound is not a predicted completion time.

The 2026-11-13 refresh adds 797 whole-test wall measurements from the nightly
collection and a completed 48-minute measurement for
`limit_zero_primary_key_analysis.xml`. That test now shares a shard with other
tests instead of being isolated. Older entries without a wall measurement retain
measured stage totals plus an estimated wrapper cost; they are not release-query
medians.

Discovery defines the test universe, so a newly added XML is included without
editing the timing table. New tests use the table's upper-quartile runtime as an
explicit estimate. Changed XML, included fragments, SQL or settings use the
larger of that estimate and its previous runtime. Deleted tests disappear automatically. New and changed
tests are listed in the job manifest; they are never assigned zero cost. Long
and shell-query XMLs retain explicit skipped entries but do not affect load.
Every discovered XML belongs to exactly one shard, including these exclusions.
The same source revision, timing table and shard count reproduce the assignment.
Updating any of them may move tests; a resumed shard rejects changed assignments.

Inspect the current deterministic assignment without starting a server:

```bash
python3 -m ci.jobs.performance_coverage_sharding --show-plan --shards 16
```

To refresh the weights, download completed shard manifests and run this from the
repository root. The command uses fresh per-test wall times from compatible
first-plus-one-warm jobs, takes the median for repeated measurements, and ignores
restored tests. Commit the updated timing table after reviewing the new balance:

```bash
python3 -m ci.jobs.performance_coverage_sharding --update-timings tmp/coverage-*/manifest.json
```

To collect just performance coverage on a PR branch, dispatch the existing
workflow with `suite=performance` (the scheduled run still collects all suites):

```bash
gh workflow run nightly_coverage.yml --ref <branch> -f suite=performance
```

To retry only missing shards on a corrected branch revision, keep the same
16-way assignment and specify their numbers:

```bash
gh workflow run nightly_coverage.yml --ref <branch> -f suite=performance -f performance_shards=4,13
```

Successful shards publish independently and do not need to be rerun. The selector
combines successful shards from separate attempts when their binary commit and
checksum, source inputs, execution policy and complete test assignment match.
Each manifest must match its own successful CIDB job, and every shard must pass
the freshness and phase-publication checks. Wrapper-only fixes may use a different
workflow commit; changed collection inputs cannot be mixed. GitHub's normal
failed-job rerun uses the original commit,
so it does not pick up collector fixes pushed afterward.

For a failed attempt that preserved completed test artifacts, add
`performance_resume_from=<full workflow commit SHA>`. The collector downloads
that shard's archive from the same branch, checks the binary revision, source
inputs, assignment, warm-run count, complete phase identities, teardown marker,
and nonempty region files, then reuses only completed whole XML tests. A failed
XML always restarts from setup. The combined manifest retains each reused test's
origin; CIDB publication still requires completion of the entire shard. New
manifests also record the binary checksum and per-test wall times.

Recovery also requires `performance_binary_url` and
`performance_binary_sha256`. Use the immutable artifact URL from the original
coverage build report and the SHA256 of its unpacked executable from a verified
manifest. The collector downloads and unpacks that binary, verifies its checksum
before initializing datasets, and records the URL in the new manifest. The build
cache may choose another executable even at the same workflow revision; recovery
must not rely on that choice. A mismatch remains an error. The checksum is not
the compressed download's checksum or its S3 ETag.

The default scheduled and manual budget is twelve hours. A single measured
`string_sort.xml` needs 6h36m51s with instrumentation; assigning it to its own
shard cannot make the complete collection fit six hours. The budget is a limit,
not a target runtime. The initial balance predicted about 6h46m for the slowest
shard, including fixture and export overhead. A complete 16-shard validation
collected all 808 eligible XML files and 7,132 queries: shard times ranged from
5h17m to 6h33m, totaling 93.4 runner-hours. New tests and runtime variation can
change that result.
`performance_max_hours=6` remains available for explicitly shorter manual runs.

```bash
gh workflow run nightly_coverage.yml --ref <branch> \
    -f suite=performance -f performance_shards=4,13 \
    -f performance_resume_from=<full-sha> -f performance_max_hours=12 \
    -f performance_binary_url=<original-artifact-url> \
    -f performance_binary_sha256=<unpacked-binary-sha256>
```

The collector reuses `perf.py` for parameter expansion, query groups, settings,
setup and teardown. It runs every expanded SQL query in each non-`long` test,
without random query sampling or the adaptive timing loop:

* `first`: one execution after test setup. This is not guaranteed to be cold:
  setup and earlier queries may have warmed OS, filesystem, or process caches.
* `warm`: one additional execution under a separate coverage identity.
  This captures repeated-execution paths, without claiming that every cache or
  compilation threshold in every test has been reached. `--coverage-warm-runs`
  changes the count for standalone collection.
* `setup` and `teardown`: separate test-wide coverage. A future selector must
  associate these regions with the whole test, rather than one arbitrary query.

A query group remains one query, matching the comparison report's query index.
The coverage identity is `<test>.query<index>.<query-sha256-prefix>.<phase>`;
the test-wide identities are `<test>.setup` and `<test>.teardown`.
Collecting per query preserves the option to select either individual queries
or whole tests later. Selection should use the union of `first` and `warm`;
separate phases let us measure the coverage gained by repeated execution.

`SYSTEM SET COVERAGE TEST` flushes LLVM counters to `system.coverage_log`, just
as in stateless tests. Attribution is process-global, so queries run serially
on one dedicated server; background work can still contribute to a phase.
The collector must not use LLVM continuous profiling (`%c`). Each successful
test exports the integration collector's six-column TSV format. The existing
exporter aggregates it into `default.checks_coverage_lines`, using the job's
start time and check name. Only completed XML tests reach the coverage upload;
partial output from failed tests remains in the diagnostic archive.

Each shard attaches `performance_coverage.tar.gz` and a job manifest. Per-test
manifests contain full expanded queries, hashes, settings, phase names and row
counts. The job manifest records the workflow and binary commits, snapshot identity, completed tests,
skips, failures and tests not reached before a timeout. `coverage_published` is
set only after successful export, independently of the job's pass/fail status;
`complete` still means every eligible test completed. The selector validates
the full shard assignment and all phases of each exported test. A missing mapping is not
evidence that a query is unaffected. Shell queries are explicitly skipped until
their subprocesses can be instrumented too; `long` tests are outside this initial
collection. Any future selector must account for these gaps and stale mappings.

To collect one test against an already configured **dedicated** instrumented
server, from the repository root:

```bash
python3 tests/performance/scripts/perf.py tests/performance/array_sort.xml \
    --host localhost --port 9000 --coverage-output tmp/perf-coverage \
    --coverage-warm-runs 1 --stop-merges --jemalloc-purge disabled
```

The mode requires `WITH_COVERAGE=ON` and `WITH_COVERAGE_DEPTH=ON` and clears
`system.coverage_log` and `system.coverage_indirect_calls` on that server.
Use a fresh output directory for each attempt. The CI wrapper is
`ci/jobs/performance_coverage.py`; local runs retain artifacts without uploading.
