## ClickHouse performance tests

This directory contains `.xml`-files with performance tests.

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

If your test takes more than 10 minutes, please, add tag `long` to have an opportunity to run all tests and skip long ones.

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
* Load raw data from S3 with `fill_query` and finish every table with the setup sequence of `tpch_sf100.xml` (insert; `MODIFY SETTING lock_acquire_timeout_for_background_operations`; `OPTIMIZE ... FINAL` with `optimize_throw_if_noop = 1`; `MATERIALIZE STATISTICS` with `mutations_sync = 1`; `SYSTEM STOP MERGES`; row count; one active part), then content checks. Statistics go before `SYSTEM STOP MERGES`, which also blocks mutations.
* Keep the tables in `default` (no `CREATE DATABASE`, no `USE`) and drop them with `DROP TABLE IF EXISTS <t> SYNC`, so the disk is free for the next test. (`USE` in existing tests is reconnect-safe: `clickhouse_driver` re-sends the database selected by `USE` when it reconnects.)
* Reuse `tests/benchmarks/` with `<query file="../benchmarks/<name>/queries/query_NN.sql"/>` and `<settings file="../benchmarks/<name>/settings.json"/>`.
* There is no skip syntax for known-problem queries: keep a working query in the test (a fixed formulation, or a placeholder such as `SELECT 'Q35 skipped'`) and describe the original query, the reason and the issue link in the benchmark's README (e.g. `tests/benchmarks/tpc-ds/README.md`). A flaky query gets an issue and is removed.

Run one test locally:

```
# Against two running servers.
tests/performance/scripts/perf.py --long --host localhost localhost --port 9001 9002 tests/performance/tpch_sf100.xml
# The whole harness as the nightly job runs it; the tested binary is ./ci/tmp/clickhouse or --ch-path <dir>.
python3 ci/jobs/performance_tests.py --test-options "arm_release, master_head, nightly, 1/1" --reference-path <older clickhouse> --test tpch_sf100
```

With the `nightly` test option the harness sorts the test list, prepends `calibration.xml` to every batch, runs all queries with `perf.py --long`, keeps the current tests for `release_base` as well (no checkout of the reference tests), and skips the performance dashboard and delta gates.

Red and green (only with `--long`). A timeout is `TIMEOUT_EXCEEDED` (for a shell query, its time limit); anything else is an error. The tested side is the new server, the reference side the old one.

| | tested side only | reference side only | both sides |
|---|---|---|---|
| prewarm timeout | red (`asymmetric timeout`) | `partial`, measured on the tested side | `double-timeout`, censored rows, no timed runs |
| prewarm error | red (`run-error` line) | `partial` | red |
| timed-run timeout | red (`asymmetric timeout`) | censored row, both sides keep running | `double-timeout`, censored rows, no more runs |
| timed-run error | red | red | red |

A censored row records the time of the completed statements plus the budget; the job result shows `double timeouts: <n>`. Regressions, `partial` queries and double timeouts are visible but do not turn the job red. Also:

* A settings or setup failure on the reference side (a feature the older binary lacks) runs the test on the tested side only, reported as `partial`, without `do_not_check_in_pr`. In the `release_base` comparison this is how tests using a setting, DDL or fill feature newer than the release appear. A setup failure on the tested side is red.
* Every test runs under `timeout -k 60` with a 3-hour budget. A non-zero exit always leaves an error record (`124`: budget expired, `137`: killed), so the job is red. A failed upload of the required results (raw runs, query metrics, test times) is red too.
* Teardown: on exit, including `SIGTERM`, `perf.py` runs the drop queries on every server that ran setup and prints `teardown-complete` only if all of them succeeded (a missing table is accepted on a reference whose setup failed). Without that line the harness records `teardown incomplete, stopping the batch` and runs no further tests on that host; what was measured is still reported.
* Changes reported by the nightly are unconfirmed: `confirm_changes` is skipped, and the repetition from night to night is the confirmation.

To get the text of a reported `query_index` (0-based, after substitutions), check out the tests of that run and use `tests/performance/scripts/perf.py --print-queries tests/performance/<test>.xml --queries-to-run <query_index>`.

#### Conditions changelog

Nightly results are comparable only under the same conditions. Append a dated line whenever any of them changes:

* YYYY-MM-DD (first scheduled night): instance type `m8g.8xlarge`, image `clickhouse/performance-comparison`, pool `arm-large-storage`.

Each job log also carries the resolved docker image digest (printed by the praktika runner) and the host identity (kernel, OS, CPU, memory, instance).

### How to run performance test

TODO

### How to validate single test

```
pip3 install clickhouse_driver scipy
../../tests/performance/scripts/perf.py --min-runs 1 --cap 1 --cap-fast 1 insert_parallel.xml
```
