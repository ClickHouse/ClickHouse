# Related Test Selection — Developer Guide

## Overview

The coverage-based related test selector (`ci/jobs/scripts/find_tests.py`) finds
stateless tests likely to catch regressions in a PR by querying a ClickHouse CI
database (CIDB) that records which lines each test covered in recent nightly runs.

---

## Architecture

### Data flow

```
NightlyCoverage CI job (nightly at 02:13 UTC)
  └─ Build amd_per_test_coverage binary
       (WITH_COVERAGE=ON -DWITH_COVERAGE_DEPTH=ON -finstrument-functions-after-inlining)
  └─ Run stateless tests including --long (clickhouse-test --collect-per-test-coverage)
       ├─ SYSTEM SET COVERAGE TEST 'test_name'  (before each test)
       └─ SYSTEM SET COVERAGE TEST ''           (flush + reset counters)
  └─ export_coverage.py (reads local server tables, inserts into CIDB)
       ├─ system.coverage_log         → checks_coverage_lines
       └─ system.coverage_indirect_calls → checks_coverage_indirect_calls
```

### CIDB tables (ClickHouse cluster, read-only via play user)

| Table | Content |
|---|---|
| `checks_coverage_lines` | Per-test file/line coverage — **main table** |
| `checks_coverage_indirect_calls` | Per-test virtual/fn-ptr callee offsets |
| `checks_coverage_inverted` | Legacy symbol→test index (not used by find_tests) |

Schema of `checks_coverage_lines`:
```sql
file LowCardinality(String), line_start UInt32, line_end UInt32,
check_start_time DateTime('UTC'), check_name LowCardinality(String),
test_name LowCardinality(String), min_depth UInt8, branch_flag UInt8
-- ORDER BY (check_start_time, file, line_start, check_name, test_name)
-- PARTITION BY toYYYYMM(check_start_time)
-- Indexes: bloom_filter on file, minmax on line_start
```

### Accessing CIDB

```python
from ci.praktika.cidb import CIDB
from ci.praktika.settings import Settings

cidb = CIDB(url=Settings.CI_DB_READ_URL, user="play", passwd="")
result = cidb.query("SELECT count() FROM checks_coverage_lines WHERE ...", log_level="")
```

---

## How find_tests.py works

```
get_all_relevant_tests_with_info()
  1. get_changed_tests()            — test files changed in the PR diff (highest priority)
  2. get_previously_failed_tests()  — tests that recently failed for this PR in CI
  3. get_most_relevant_tests()      — coverage-based (6 passes, see below)
```

### Previously-failed tests

Queries CIDB `checks` table for tests that failed in earlier CI runs for this PR:

```sql
SELECT test_name
FROM checks
WHERE pull_request_number = {PR_NUMBER}
  AND check_name LIKE '{JOB_TYPE}%'
  AND check_status = 'failure'
  AND test_status = 'FAIL'
  AND check_start_time >= now() - interval 30 day
GROUP BY test_name
ORDER BY count() * exp(-dateDiff('day', max(check_start_time), now()) / 7.) DESC
LIMIT 100
```

- Ordered by recency-weighted failure count (7-day exponential decay half-life)
- 30-day window covers the full PR development cycle
- Capped at 100 tests
- Supported job types: `Stateless` (pattern `^[0-9]{5}_`) and `Integration` (pattern `^test_`)

### Coverage-based selection — 6 passes

**Pass 1 — Direct line coverage** (`get_tests_by_changed_lines`):
- Gets changed `(file, line_no)` pairs from `gh pr diff`
- Filters to `src/`, `programs/`, `utils/`, `base/` (COVERAGE_TRACKED_PREFIXES)
- Queries `checks_coverage_lines` for regions covering those specific lines
- Skips regions where `region_test_count > MAX_TESTS_PER_LINE (200)` to avoid
  flooding from ultra-broad files like `Context.cpp`
- Weight: `PASS_WEIGHT_DIRECT = 1.0`

**Pass 1b — Hunk context** (within same pass):
- Context lines adjacent to changed lines within the same diff hunk
- Lower weight to rank below direct hits but above indirect/sibling
- Weight: `PASS_WEIGHT_HUNK_CONTEXT = 0.50`

**Pass 2 — Sibling directory** (`_query_sibling_dir_tests`):
- Extracts domain keywords from changed filenames (CamelCase split, strips common words)
  - e.g. `CHColumnToArrowColumn.cpp` → `["Arrow"]`
  - e.g. `MergeTreeIndexConditionText.cpp` → `["Index", "Text"]`
- Finds sibling files in the same directory covered by the primary tests
- Finds OTHER tests covering those sibling files
- Synthetic `SIBLING_DIR_WIDTH = 3000` (ranks below direct hits)
- Weight: `PASS_WEIGHT_SIBLING = 0.25`

**Pass 3 — Indirect callee co-occurrence** (`_query_indirect_call_tests`):
- Self-joins `checks_coverage_indirect_calls` on `callee_offset`
- Finds tests sharing virtual/function-pointer callees with primary tests
- Adaptive Jaccard threshold: `max(1, 70 - (200 - min_seed_rc) × 0.5)%`
  - Ranges from 15% (very specific files, rc=1) to 70% (broad files, rc≥200)
- Adaptive limit: `max(50, min(200, 200 × min_seed_rc / 40))` tests
- Callees appearing in ≥300 tests excluded (ubiquitous: logging, malloc)
- Weight: `PASS_WEIGHT_INDIRECT = 0.50`

**Pass 4 — Broad-tier2** (within direct query pass):
- Tests covering changed files via broad regions (rc 2001–8000)
- Ranked by `cov_regions × files_covered` (prefer tests covering more of the change)
- Weight: `PASS_WEIGHT_BROAD2 = 0.40`

**Pass 5 — Sparse-file expansion**:
- Triggered when a changed file has very few direct hits (max rc ≤ 8)
- Fetches all narrow regions (rc ≤ MAX_TESTS_PER_LINE) in the changed file
- Supplements the indirect callee seeds with other narrow regions from same file
- `SPARSE_FILE_WIDTH = 600` (between direct and sibling)
- Weight: `PASS_WEIGHT_SPARSE_FILE = 0.30`

**Fallback — Keyword matching**:
- Used when coverage passes return few/no results
- Matches test filenames against domain keywords from changed files
- Top-30 tests by keyword specificity score
- Weight: `PASS_WEIGHT_KEYWORD = 0.20`

### Scoring formula

```
score(test) = sum over all matched changed lines of:
    pass_weight / (region_width × region_test_count)
```

Where:
- `region_width` = lines in the coverage region (narrow = high signal)
- `region_test_count` = tests covering this region (specific = high signal)
- `pass_weight` = per-pass multiplier (see above)

**Ranking tiers** (applied before score sort):

| Tier | Condition |
|---|---|
| **A** (best) | Narrow region (`width ≤ NARROW_REGION_MAX_LINES = 40`) AND shallow depth (`depth ≤ DIRECT_CALL_MAX_DEPTH`) |
| **B** | Narrow region, deep call path |
| **C** | Broad region only |

**Output filtering**:
- `MIN_SCORE = 1e-8` — absolute floor
- `MAX_SCORE_RATIO = 3000` — drop tests scoring >3000× below top direct hit
- `effective_min = min(1e-6, max(1e-8, top_score / 3000))`
- `MAX_OUTPUT_TESTS = 300` — hard output cap

### Key constants (ci/jobs/scripts/find_tests.py)

```python
MAX_TESTS_PER_LINE      = 200    # skip regions covered by >200 tests (direct pass)
BROAD_REGION_HARD_CAP   = 2000   # server-side HAVING on direct query
VERY_BROAD_REGION_CAP   = 8000   # broad-tier2 upper bound
MAX_OUTPUT_TESTS        = 300    # final ranked cap
MAX_CALLEE_TEST_COUNT   = 300    # indirect callee ubiquity filter
JACCARD_MIN_PCT         = 70     # base indirect callee Jaccard threshold (adaptive)
MAX_KEYWORD_TESTS       = 30     # keyword fallback cap
NARROW_REGION_MAX_LINES = 40     # tier-A/B boundary
SIBLING_DIR_WIDTH       = 3000   # synthetic width for sibling tests
SPARSE_FILE_WIDTH       = 600    # synthetic width for sparse-file expansion
SPARSE_FILE_THRESHOLD   = 8      # max rc to trigger sparse-file pass
BROAD_FALLBACK_WIDTH    = 100    # effective width for broad-tier2 tests
BROAD_TIER2_RC          = 600    # synthetic rc for broad-tier2 scoring
```

### Known limitations

| Change type | Result | Why |
|---|---|---|
| `constexpr`/`static const` at declaration | 0 tests | Compile-time, no runtime counter |
| Ultra-broad infrastructure file (`IMergeTreeDataPart`, `Context`) | capped | rc > 8000 excluded; MAX_OUTPUT=300 cuts many ranked tests |
| Niche C++ (LDAP, CLI, crash handlers) | 0 or keyword-only | Files not covered by stateless suite |
| PR > 30 days old | degraded | CIDB retention window |
| Long tests | now covered | `--no-long` removed from per-test coverage runs |
| New test added in same PR | correctly detected | `get_changed_tests()` picks it up; not yet in CIDB |

---

## Quality metrics (as of 2026-03-30, 100 PRs)

Analysis against 100 merged PRs (2026-03-25 to 2026-03-28) with `src/` changes,
compared against old DWARF-based algo targeted runs:

| Metric | Value |
|---|---|
| Mean recall vs old algo | 57.8% |
| Median recall | 65.2% |
| Weighted recall | 49.2% |
| Perfect recall (100%) | 10/28 PRs with targeted data |
| Small PRs (old ≤10 tests) | 68.9% mean |
| Large PRs (old >10 tests) | 45.0% mean |

**Root causes of misses:**
- ~40% previously-failed tests (only appear during active PR development, not reproducible after merge)
- ~25% rc > 8000 infrastructure files (excluded by `VERY_BROAD_REGION_CAP`)
- ~15% `MAX_OUTPUT=300` cap (tests ranked but cut)
- ~14% DWARF false positives in old algo (old found wrong tests via inline chains — new algo is correct to skip)
- ~2% new test files added in same PR (not yet in CIDB)
- ~4% other genuine misses

81% of "missed" tests (333/411 checked pairs) ARE genuinely in `checks_coverage_lines`
for the changed files — the new algo finds different-but-valid subsets for broad files.

See `.claude/docs/compare_find_tests_algos.md` for the comparison methodology.

---

## Running find_tests locally

```bash
cd /path/to/ClickHouse
# Get related tests for a PR
PYTHONPATH=./ci:. python3 ci/jobs/scripts/find_tests.py <PR_NUMBER>

# Skip previously-failed pass (faster, coverage-only)
PYTHONPATH=./ci:. python3 ci/jobs/scripts/find_tests.py <PR_NUMBER> --coverage-only

# Pre-fetched diff (avoids GitHub API rate limit)
gh pr diff <PR_NUMBER> > /tmp/pr.diff
PYTHONPATH=./ci:. python3 ci/jobs/scripts/find_tests.py <PR_NUMBER> --diff-file /tmp/pr.diff
```

Example output:
```
[find_tests] sibling-dir query: 0.15s, response=175 bytes
[find_tests] sibling-dir: 4 additional test candidates
[find_tests] indirect-call query: 1.88s, 200 additional test candidates (top jaccard=100%)
[find_tests] done in 2.66s: 6/6 lines matched, 275 unique tests selected
All selected tests (275):
 00900_long_parquet.sh
 ...
Found 275 relevant tests
```

---

## NightlyCoverage workflow

Defined in `ci/workflows/nightly_coverage.py`. Runs nightly at 02:13 UTC on master.
Uses `coverage_build_jobs[1]` = `Build (amd_llvm_coverage_per_test)` which builds
with `WITH_COVERAGE=ON -DWITH_COVERAGE_DEPTH=ON -finstrument-functions-after-inlining`.

**Long tests are included** (removed `--no-long` from per-test coverage runs in
`ci/jobs/functional_tests.py`), so tests like `00900_long_parquet` and
`02340_parts_refcnt_mergetree` now appear in `checks_coverage_lines`.

Trigger manually on a branch:
```bash
gh workflow run NightlyCoverage --repo ClickHouse/ClickHouse --ref <branch>
gh run list --repo ClickHouse/ClickHouse --workflow=NightlyCoverage --limit 5
```

---

## Key files

| File | Purpose |
|---|---|
| `ci/jobs/scripts/find_tests.py` | Main algorithm — `Targeting` class |
| `ci/jobs/functional_tests.py` | Runs tests; calls `get_all_relevant_tests_with_info()` |
| `ci/jobs/scripts/functional_tests/export_coverage.py` | Exports local coverage tables to CIDB |
| `ci/workflows/nightly_coverage.py` | NightlyCoverage workflow definition |
| `base/base/coverage.cpp` | Runtime: reads LLVM profile data, resets per-test, collects indirect calls |
| `src/Common/CoverageCollection.cpp` | Server-side: maps counters to source regions, inserts into system tables |
| `src/Common/LLVMCoverageMapping.cpp` | Parses ELF `__llvm_covmap`/`__llvm_covfun` sections at startup |
| `tests/clickhouse-test` | Creates `system.coverage_log` and `system.coverage_indirect_calls` tables |

---

## Indirect call collection details

LLVM value profiling (`-enable-value-profiling=true`) records runtime callee addresses
at each virtual call / function pointer site into `ValueProfNode` linked lists.
**Critical**: `__llvm_profile_reset_counters()` does NOT reset these nodes.
The fix in `base/base/coverage.cpp` zeros `node->count` after reading so each test
starts fresh — without this, every test accumulates all prior tests' indirect calls.

The `callee_offset = callee_address − binary_load_base` is stable across ASLR
restarts for the same binary build. The find_tests self-join query uses this:
```sql
SELECT DISTINCT ic2.test_name
FROM checks_coverage_indirect_calls ic1
JOIN checks_coverage_indirect_calls ic2 ON ic1.callee_offset = ic2.callee_offset
WHERE ic1.test_name IN ({primary_tests})
  AND ic2.test_name NOT IN ({primary_tests})
  AND (ic2.test_name, ic1.callee_offset) IN (
      SELECT test_name, callee_offset FROM checks_coverage_indirect_calls
      GROUP BY test_name, callee_offset
      HAVING uniqExact(test_name) < 300   -- exclude ubiquitous callees
  )
  AND count(DISTINCT ic1.callee_offset) * 100.0 / ic2_tot.tot_callees >= {JACCARD_MIN_PCT}
LIMIT {INDIRECT_LIMIT}
```

---

## CIDB data freshness check

```python
PYTHONPATH=./ci:. python3 - << 'EOF'
from ci.praktika.cidb import CIDB
from ci.praktika.settings import Settings
cidb = CIDB(url=Settings.CI_DB_READ_URL, user="play", passwd="")

# Check freshness and coverage of long tests
print(cidb.query("""
SELECT check_name, max(toDate(check_start_time)) AS last_run,
       uniqExact(test_name) AS tests, count() AS rows
FROM checks_coverage_lines
WHERE check_start_time > now() - interval 7 days
GROUP BY check_name
ORDER BY last_run DESC
LIMIT 10
""", log_level=""))

# Verify a specific long test appears for a file
print(cidb.query("""
SELECT test_name, count() AS regions
FROM checks_coverage_lines
WHERE file = './src/Storages/MergeTree/MergeTreeRangeReader.cpp'
  AND check_start_time > now() - interval 7 day
  AND check_name LIKE 'Stateless%'
  AND test_name LIKE '02340%'
GROUP BY test_name
""", log_level=""))
EOF
```
