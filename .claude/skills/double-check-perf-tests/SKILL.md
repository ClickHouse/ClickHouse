---
name: double-check-perf-tests
description: Locally reproduce ClickHouse CI performance comparison results for a given commit. Fetches the perf CI report, identifies queries categorized as "Changes in Performance", downloads both the patched and reference binaries from S3 (matching the current machine architecture), and re-runs only those queries via `tests/performance/scripts/perf.py` to verify whether each regression/improvement is real. Use this whenever the user wants to "double-check", "reproduce", "verify locally", or "re-run" a perf check result — even if they don't name the skill.
argument-hint: "<commit-sha> [--db-path <path>]"
allowed-tools: Bash, Read, Grep, Glob, WebFetch
---

# Double-check ClickHouse perf-test results

## What it does

Given a commit SHA from a PR that ran the Performance Comparison check, this
skill:

1. Looks up the PR for the commit (via `gh api`).
2. Detects the local machine's architecture (`amd` / `arm`).
3. For each perf shard, fetches `report.html` and extracts the rows in the
   "Changes in Performance" table (`<tr id="changes-in-performance.<test>.<idx>">`),
   then pulls the timing numbers for those rows from `all-query-metrics.tsv`.
   This matches the report exactly — re-implementing compare.sh's
   `changed_show` predicate locally would require historical thresholds
   and per-test `<report_threshold>` settings we don't have on the client side.
4. Runs *every* CI-flagged query locally — even ones flagged only on a
   different architecture. Cross-arch changes still get measured against
   the same two binaries; the report table tags each row with a "CI@"
   column showing which arch(es) CI flagged the query on (e.g. `arm-only`
   means CI saw the change on ARM but the local rerun is on AMD). This
   surfaces silent drift on the local arch and lets the user judge whether
   an `<arch>-only` CI verdict was real or noise.
5. Resolves the reference (left/baseline) git SHA used by the CI run by
   querying `query_metrics_v2` on `play.clickhouse.com` for the row with
   `new_sha = <pr-sha>` (the `report.html` "Tested Commits" section is
   unreliable — for official builds `clickhouse --version` does not embed
   the git hash). A commit that was measured more than once has one
   reference per run, so the newest one is taken — that is the run the
   S3 report reflects, since it is overwritten in place — and the script
   warns when the choice was not unique.
6. Downloads both binaries from `clickhouse-builds`:
   - Right: `PRs/<pr>/<sha>/build_{amd,arm}_release/clickhouse`
   - Left:  `REFs/master/<ref-sha>/build_{amd,arm}_release/clickhouse`
7. Starts two local `clickhouse-server` processes (ports 9001 + 19001, the
   same ports `CHServer` uses in `ci/jobs/performance_tests.py`).
8. Reruns **only** the affected query indices via
   `tests/performance/scripts/perf.py` for each affected XML.
9. Prints a side-by-side comparison: CI numbers vs. local numbers, with a
   verdict per query (`CONFIRMED slower`, `NOT REPRODUCED`, `no local data`).

## Arguments

- `$0` (required): commit SHA (full or short — `gh` resolves short hashes).
- `--db-path PATH` (optional): directory with the standard perf datasets
  loaded (`hits10`, `hits100`, `hits_v1`, `values`, `tpch10`, `tpcds1`).
  Must match the layout of `ci/tmp/perf_wd/db0`. If omitted, the script
  probes `ci/tmp/perf_wd/db0`.
- `--pr N`, `--reference-sha SHA`: override auto-detection.
- `--runs N`: number of measurements per query (default 7, matches CI).
- `--populate`: rebuild the affected `hits` tables on each server
  separately, the way CI's `populate_data_both` does, instead of sharing
  one hardlinked copy. See "Hardlinked data vs. `--populate`" below.
- `--no-cpu-pinning`: don't pin the servers with `taskset` and don't cap
  `max_threads`. Only for a machine where pinning is undesirable — it
  measures under noisier conditions than the report being checked.
- `--dry-run`: stop after resolving PR / SHAs / changed queries; do not
  download or run.

## Procedure

### 1. Sanity checks

- The skill must be invoked from the root of a ClickHouse checkout (the
  script verifies `tests/performance/scripts/perf.py` is present).
- The dry-run inspects every affected XML and prints the list of external
  datasets it actually references (`hits_*`, `test_values`, `tpch.*`,
  `tpcds.*`). Most perf tests are self-contained — they `CREATE TABLE … FROM
  numbers(…)` and need no preloaded data at all. **Only require the datasets
  the changed queries truly use; do not insist on the full 50 GB bootstrap.**
- If the affected XMLs reference zero external datasets, the script creates
  an empty `ci/tmp/perf_wd/db0` automatically and proceeds.
- If they do reference one or more external datasets and `ci/tmp/perf_wd/db0`
  is missing, the script bails with the *minimal* list of tarball URLs
  needed for this particular run. Ask the user before downloading. Example:
  if the only affected XML uses `hits_100m_single`, just fetch that one
  tarball (~10 GB) into `ci/tmp/perf_wd/db0`, not all six.

  ```bash
  mkdir -p ci/tmp/perf_wd/db0/data/default
  # extract only the tarballs the dry-run identified as needed
  wget -nv -nd -c "<url-from-dry-run>" -O- | tar --extract -C ci/tmp/perf_wd/db0
  ```

  Do **not** auto-download — confirm with the user first.

### 2. Resolve the commit

Always run a dry-run first so the user can sanity-check what's about to be
rerun before any download starts:

```bash
python3 .claude/skills/double-check-perf-tests/double_check_perf.py <commit-sha> --dry-run
```

This prints: PR number, architecture, reference SHA, and the list of
affected XML files with their changed query indices. If anything looks
wrong (wrong arch, wrong reference SHA, wrong PR), pass `--pr` /
`--reference-sha` to override.

### 3. Run the comparison

```bash
python3 .claude/skills/double-check-perf-tests/double_check_perf.py <commit-sha>
```

Working directory defaults to `tmp/double_check_perf/` in the cwd (per
`CLAUDE.md`: don't use `/tmp`). It contains:

- `left/clickhouse`, `right/clickhouse` — downloaded binaries, each with
  a `.identity` file recording the SHA it was built from. The work dir is
  shared across runs, so a cached binary is reused only when it is the one
  the current invocation asked for; a different commit or
  `--reference-sha` re-downloads.
- `left/db/`, `right/db/` — hardlinked dataset copies
- `left/server.log`, `right/server.log` — server logs
- `raw/<test>-raw.tsv` — `perf.py` output per test
- `result.json` — structured result of the local rerun

### 4. Present the results

The script prints a table. For each changed query show:

- CI old / new / Δ (from the report)
- Local old / new / Δ / p-value (from `perf.py`)
- Verdict: `CONFIRMED slower|faster`, `NOT REPRODUCED`, or `no local data`

A query counts as `CONFIRMED` when the local rerun shows the same direction,
clears the **same per-query threshold CI used to flag it**, and is
statistically significant (`p <= 0.05`, the cutoff `perf.py` itself uses).
Anything else is `NOT REPRODUCED`, and the verdict says which of the three
conditions failed.

The threshold is not a fixed number: `compare.sh` computes it per query as
the 0.15 floor raised by the query's historical p99 and the test's
`<max_ignored_relative_change>`, and exports it as the `changed_threshold`
column of `all-query-metrics.tsv`. A historically noisy query therefore has
to clear a much larger bar than a stable one. Using a flat bar instead would
let the rerun call a change `CONFIRMED` that CI's own gate would not have
flagged — the floor alone is deliberately above the 10–15% that micro
benchmarks swing between two binaries from machine noise and code layout.
Shards predating the column fall back to the 0.15 floor.

When summarising back to the user, separate the confirmed regressions /
improvements from the not-reproduced cases. Confirmed regressions are the
ones worth investigating further; not-reproduced ones can usually be
treated as CI noise.

## Notes

- The same `tests/performance/scripts/perf.py` and the same drop-in config
  files (`tests/performance/scripts/config/{config.d,users.d}`) are used as
  in CI, so the run is as close to CI as possible without Praktika. The
  ports and shared dataset directory match `CHServer` in
  `ci/jobs/performance_tests.py`.
- **CPU pinning.** On Linux x86_64, CI pins both servers with `taskset` to
  one hyperthread per physical core and caps `max_threads` at the size of
  that set, so query threads never share a hyperthread sibling depending on
  scheduler mood — CI's top suspect for the amd-vs-arm A/A noise gap (0.51%
  vs 0.42%). The script does the same, including the same
  `--jemalloc_profiler_sampling_rate`. This matters for the verdicts: an
  unpinned rerun is noisier than the report it is adjudicating, which is how
  a real change ends up looking `NOT REPRODUCED`. `arm` runs on real cores
  and is not pinned, in CI or here.
- The reference (left) binary's git hash is resolved via
  `play.clickhouse.com` (anonymous `explorer` user, no credentials needed),
  using the `query_metrics_v2.old_sha` column for the matching `new_sha`
  and `arch`. If that query fails or returns nothing (e.g. the run never
  finished uploading), pass `--reference-sha` explicitly. The CI sets this
  field from `SELECT value FROM system.build_options WHERE name='GIT_HASH'`
  on the reference binary itself, so the resulting SHA is guaranteed to
  match a buildable commit under `REFs/master/<sha>/build_*_release/`.
- Datasets are intentionally not downloaded automatically — they are large
  and the user should opt in. Existing data is hardlinked into both server
  dirs via `cp -al` (same trick `performance_tests.py` uses), so disk usage
  stays low.
- The perf framework expects `test.hits` (not `datasets.hits_v1`) for
  several tests (`url_hits`, `count_from_formats`, ...). By default the
  script runs a temporary "preconfig" `clickhouse-server` pointed at `db0`
  and issues `CREATE DATABASE test; RENAME TABLE datasets.hits_v1 TO
  test.hits` via SQL, so one copy of the data is shared by both sides.
  (`ci/jobs/performance_tests.py` instead builds `test.hits` with
  `INSERT SELECT` on each server — that is what `--populate` reproduces,
  and under `--populate` this rename is skipped so the source table stays
  available to both sides.) Doing this via
  filesystem-only moves of the .sql files looks equivalent but leaves
  bookkeeping in a state that crashes the next server start while
  loading `tpcds` (NULL deref in
  `DatabaseOrdinary::getConvertToReplicatedFlagPath`). Always use the SQL
  path. Step is idempotent — skipped if `test.hits` already exists in
  `db0`. After the preconfig server exits, the script strips
  `data/system`, `metadata/system`, `status`, `preprocessed_configs` from
  `db0` since those are per-server state that mustn't be shared between
  the left/right hardlinked copies.
- **Hardlinked data vs. `--populate`.** By default both servers read one
  hardlinked copy of `db0` (`cp -al`, the same trick
  `performance_tests.py` uses), so the parts they read were written by
  whatever binary produced the dataset tarball. CI does not do this: its
  `populate_data_both` re-inserts `hits_10m_single`, `hits_100m_single`
  and `datasets.hits_v1` → `test.hits` on each server, so each side's
  parts carry that side's own write-time defaults (sparse columns,
  statistics, mark format). A regression that lives in the write path,
  or one that only shows on freshly written serialization, therefore
  comes back `NOT REPRODUCED` under the default. Pass `--populate` to
  reproduce CI faithfully; it only rebuilds the `hits` tables the
  affected XMLs actually reference, but each one is a full rewrite per
  side (`hits_100m_single` alone is ~21 GiB and tens of minutes) and
  gives up the hardlink disk saving for those tables. When a confirmed CI
  regression does not reproduce and the PR touches anything on the write
  path, rerun with `--populate` before calling it noise.
- Between test XMLs, everything a test wrote into `user_files` is removed
  from both sides while the seeded fixture symlinks are kept — the same
  cleanup CI runs after every test. Tests write there with `INSERT INTO
  FUNCTION file(...)` (`parquet_read`, `json_type_parsing`,
  `insert_values_with_expressions`, ...) and `drop_query` only drops tables,
  so without it a later XML can read what an earlier one left behind and a
  multi-test rerun becomes order-dependent.
- The skill does **not** attempt to reproduce flamegraphs or profiling —
  for that, use the `perf-report` skill on the same PR.
- Architecture mismatch is partial: if a PR has only ARM shards and you're
  on AMD, the script bails (it needs at least one shard for the local arch
  to compare against). If both archs exist, the script runs queries
  flagged on *either* arch locally — the CI@ column tells you which arch
  CI flagged each query on. For the strictest verification, run the skill
  on each arch separately; otherwise the AMD rerun of an ARM-only change
  is still useful ("local AMD doesn't reproduce the ARM regression" is a
  meaningful and common verdict).
- Some shards upload `all-query-metrics.tsv.zst` (zstd-compressed) instead
  of plain `.tsv` — the script detects the URL suffix and decompresses on
  the fly (uses the `zstandard` Python package if available, else shells
  out to `zstd -dc`).
- **Wait for merges before measuring on freshly-loaded data.** A dataset
  tarball drops parts at whatever merge level the snapshot was taken;
  ClickHouse queues consolidation merges on startup. While those run the
  number of parts drifts (changes plans, prefetch, external-storage cache
  reuse) and the merge threads themselves compete for CPU/IO with the
  queries being timed. The signal we look for is *no new merge scheduled*:
  the script polls `SELECT min(elapsed) FROM system.merges` on both servers
  and considers them settled once the youngest in-flight merge has been
  running for at least 2 minutes (so nothing new has started in that
  window). This is more useful than waiting for `count()=0`: long-running
  merges can stretch that wait by tens of minutes for no real gain once
  the *rate* of new merges has dropped to zero. Pass
  `--skip-wait-for-merges` only when reusing a perf working directory
  that already settled in a previous run.

## Rules

- Always run `--dry-run` first and show the user the plan before downloads.
- When datasets are missing, **ask** the user before bootstrapping. Don't
  silently fire off multi-GB downloads.
- Don't truncate or summarize the result table — every changed query must
  be visible, same principle as the `perf-report` skill.
- If the script reports `NOT REPRODUCED` for a query that has a large CI
  delta, suggest re-running with `--runs 13` (more samples) before
  declaring it flaky. If the PR changes anything that affects how parts
  are written, suggest `--populate` too — the default hardlinked dataset
  cannot show a write-path change at all.
