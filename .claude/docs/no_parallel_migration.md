# Making `no-parallel` Stateless Tests Parallel-Safe

Notes from https://github.com/ClickHouse/ClickHouse/pull/110015.

The `no-parallel` tag makes a test run alone, after the whole parallel phase has finished, on a single worker. On master 876 of 13004 stateless tests carry it, forming a long sequential tail in every functional-test job. Most of those tags predated the per-test isolation primitives and were either stale or fixable mechanically.

Counted on the branch as of this writing: **206 still run solo and 22 run in concurrency groups**, so roughly three quarters of the tail is gone. Do not trust the round numbers in the pull request description — they were written early and several tags were deliberately restored during the fix rounds, so recount before quoting.

---

## What Landed

**Runner** (`tests/clickhouse-test`):

- `no-parallel:<group>` concurrency groups. Each named group is a serial queue consumed by one parallel worker, so members never overlap each other but do run alongside the rest of the suite. Plain `no-parallel` keeps the old strictly-solo semantics. Group names are validated against `NO_PARALLEL_GROUPS`.
- Typed per-run identities for `.sql` tests as query parameters (`{CLICKHOUSE_TEST_UNIQUE_NAME}`, `{CLICKHOUSE_TEST_UUID}`, `{CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}`, `{CLICKHOUSE_USER_FILES}`) and the matching environment variables for `.sh` tests.
- A per-test `user_files` sandbox (`CLICKHOUSE_USER_FILES_UNIQUE`), created and removed by the runner.
- `--no-self-parallel`, so repeated copies of one test do not overlap in the flaky and targeted checks. This is the right tool when a test's isolation is per test file rather than per run.
- A global-entity leak check: after a test, the runner looks for users, roles, quotas, row policies, settings profiles, functions, named collections and workloads whose name contains the test's unique name.

**Server**: `SYSTEM CLEAR <cache> CACHE FOR TABLE [db.]table` for the part-keyed caches and the query condition cache, so a test can reset its own cache state without disturbing concurrent tests. The `table_uuid` column of `system.query_condition_cache` is now available in all builds so tests can filter it to their own tables (`part_name`, `condition` and `condition_hash` stay behind `DEBUG_OR_SANITIZER_BUILD`).

**Style checks**: a `no-parallel` tag is required only for *unscoped* cache clears, group names must come from the allowlist, and every `no-parallel` tag must document its reason in a `Tag no-parallel:` comment.

---

## Migration Recipes

| Conflict | Fix |
|---|---|
| Fixed table, database or ZooKeeper path names | unique per-run names from `CLICKHOUSE_TEST_UNIQUE_NAME` / `CLICKHOUSE_TEST_ZOOKEEPER_PREFIX` |
| Reads and writes under `user_files` | the per-test `user_files` sandbox |
| `SYSTEM DROP <cache> CACHE` | scoped `SYSTEM CLEAR <cache> CACHE FOR TABLE` |
| Query cache | `query_cache_tag` plus `SYSTEM CLEAR QUERY CACHE TAG` |
| Scans of `system.query_log` and friends | filter by `current_database`, `log_comment` or a unique query id |
| Assertions on global counters | per-query `ProfileEvents` from `system.query_log` |
| Global entities (users, roles, functions, …) | test-unique names plus an explicit drop |

The tests that legitimately stay solo do so for a documented reason, enforced by the style check: failpoints, `SYSTEM RELOAD CONFIG`/`USERS`, XRay instrumentation, root workload mutations, wall-clock and saturation tests, and global gauge or `last_error_*` semantics. Only four of the eight allowlisted groups are actually used — `metadata-caches` (8 tests), `misc-caches` (8), `filesystem-cache` (4) and `xml-entities` (2).

**Do not remove a `no-parallel` tag without changing the test.** Several tests in this PR had the tag stripped while the body stayed byte-identical; master later re-added the tag to one of them (`04051_pk_analysis_stats`) with a correct explanation, and the merge had to take master's side. Conversely, check whether the tag is already obsolete: `02350_views_max_insert_threads` had carried it for years, but `use_concurrency_control=0` had made it unnecessary months earlier — only the stale comment needed removing.

---

## Problems Found and How They Were Fixed

### Tests asserting on process-wide, size-limited caches

The largest recurring class. The query cache, query condition cache, mark cache and filesystem cache are all shared and evictable, so once a test stops running solo a concurrent test can evict its entries between the query that populated one and the assertion that reads it. A hit becomes a miss, a count comes out low.

- **Four QCC TopK tests** (`04217`, `04242`, `04320`, `04338`) asserted `count()` of `system.query_condition_cache`, giving `-4 / +1` style diffs. Fixed by snapshotting entries into a per-test `Memory` table right after each query and asserting `uniqExact(key_hash)` over the accumulated union: an eviction after an entry is recorded can no longer change the result, and a re-written entry keeps the same `key_hash`. `key_hash` is the only entry identity available in every build. All expected values were unchanged, so no reference needed touching.
- **`02494_query_cache_query_log`** asserted that a second run of a query recorded `query_cache_usage = 'Read'`. Pinning one run's usage is not observable on a shared cache. Fixed by repeating the query and asserting that across the repeats the cache was both written and read at least once.
- **`02240_filesystem_cache_bypass_cache_threshold`** is the indirect form, and the most interesting one. It asserted that a second read after a cache clear cached nothing. That only held because the marks of the compact part were still resident in the **mark cache**, so the read touched no file below `bypass_cache_threshold`. Under parallelism the marks were evicted, re-read from disk, and an 80-byte segment reappeared. Fixed by restricting the assertion to above-threshold segments, which is what the test is named for; the sub-threshold segment is still asserted after the first, cold read.

The general rule: never assert an exact hit, entry count, or "nothing was cached" against one of those caches. Assert something eviction-tolerant instead — a union of observations, "at least one hit across N repeats", a filter to the property under test, or per-query `ProfileEvents`.

### Runner bugs introduced by the migration itself

- **The post-run leak check was silently dead.** It put every unique name of the run into a single query, and on a full shard the list overflowed the server's limit on one HTTP form field: `Code: 500 ... HTML Form Exception: Field value too long`. Now chunked at 500 names.
- **The per-test leak check was expensive.** One query over eight system tables for every test. Comparing job durations against master, the parallel phase of every configuration was 20–43% slower while the sequential phase was much faster. Gating the check on whether the test text mentions global-entity DDL cut it to 378 of 12887 files, and `amd_tsan, parallel` went from 6584s to 6027s. A cross-check confirmed the gate is sound: every test in the suite that *drops* a global entity also matches the create pattern.
- **Nine of 24 Fast test workers died mid-run.** The `UnicodeDecodeError` path of the tag reader returned a one-element *list*; the caller's `tags or set()` only rescues a falsy value, so it reached `TestCase.__init__`, which calls `self.tags.add(...)`. Each dead worker took the rest of its queue with it, so tests silently went unrun and `clickhouse-test` exited 1 with no failing test to point at.
- **The two group allowlists drifted.** `remote-databases` was removed from `NO_PARALLEL_GROUPS` in the runner but left in the style check's list, so the check would accept a group name the runner rejects — and `get_no_parallel_group` raises while the suite is built, aborting the whole run rather than reporting a style violation.

### Runtime-aware shard batching: tried, measured, removed

A checked-in duration manifest plus longest-processing-time-first assignment replaced name hashing for splitting tests across shards. It was removed again. The manifest held 27 entries for 13034 tests, so the LPT step only placed those 27 and the remaining ~13000 were balanced by count, much as the hash already did; measured balance for 2 shards was 1 test and 60s of known weight out of ~1000s. Nothing regenerated the manifest either. The suspicion that it caused a shard timeout was also wrong — the split was even.

---

## Traps

**Comments in a `.sql` test that uses `-- { echo }`.** Keep any explanatory comment block directly below the `-- Tags:` line with no blank line between them. `getTestTagsLength` (`src/Client/TestTags.cpp`) strips only a contiguous run of `--` comments starting at the `Tags:` line, and the echo marker prints the first query together with every comment preceding it since the last statement. A comment placed after a blank line is echoed into the output and the test fails its diff on line 1. Region-based `-- { echoOn }` / `-- { echoOff }` does not have this problem, and neither does a comment with a statement between it and the marker.

**Query parameters have limited positions.** `{CLICKHOUSE_TEST_UNIQUE_NAME:String}` and friends are accepted only in expression positions such as a `WHERE` clause. They do not work as the value of a `SETTINGS` clause (so they cannot name an inline `disk(...)`), nor as the argument of `SYSTEM CLEAR FILESYSTEM CACHE '<name>'` or `SYSTEM ... TAG`. A test needing a per-run unique name in one of those positions has to be a `.sh` test interpolating `$CLICKHOUSE_TEST_UNIQUE_NAME`.

**No CI at all usually means a merge conflict.** A branch that conflicts with master reports `CONFLICTING` from `gh pr view <n> --json mergeable,mergeStateStatus`, and GitHub then cannot build the merge commit, so no `pull_request` workflow is triggered and the newest run stays pinned to an older commit. This silently cost two rounds of pushes here.

**A cancelled job is reported as `fail`.** `gh api repos/ClickHouse/ClickHouse/actions/runs/<run>/jobs` shows a `Run` step whose `conclusion` is `null`, the duration is a round number of minutes, no artifacts reach S3, no row reaches CIDB, and the Praktika report still says `RUNNING`. Pushing a new commit cancels the in-flight run and produces exactly this signature, so weigh a push against the results you are about to discard.

**Read the runner's rerun verdict first.** A failing test is re-run with the same randomized settings and the report records `Runs: N, Failed: 0 ... All reruns passed`. A test that survives 100+ reruns is transient; one that fails every attempt is worth chasing even when it surfaced in a flaky check, which is how `02240` and `02494` were caught.

---

## Still Open

- **Darwin `UNKNOWN_DATABASE`.** On one `Fast test (arm_darwin)` run, 9 tests failed instantly with `Database test_xxx does not exist`. The server log proves the `CREATE DATABASE` request never arrived (9975 requests for 9982 tests), yet the tests ran, and `clickhouse_execute` raises on every failure path. The only branch that skips the create requires `args.database` to be set, and nothing assigns it. Not reproduced since.
- **Parallel phase versus sequential phase.** The sequential phase got 37–65% faster and the parallel phase 20–43% slower, so for configurations where the parallel phase already dominated, the critical path got worse even though total machine time improved.
- **`02352_interactive_queries_from_file.expect`** has a lowercase `# tags: long, no-parallel`, and the runner only recognises `Tags:`, so both tags are silently ignored. Pre-existing on master. It is the only such file in the suite; fixing the typo would effectively re-enable `no-parallel`.
