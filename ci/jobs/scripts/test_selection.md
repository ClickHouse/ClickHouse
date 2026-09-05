# Precise stateless selection

`Select functional tests` produces a single manifest for a PR SHA and selector
version. A conditional S3 insert makes the first successful manifest immutable;
retries and all selected/targeted configurations consume the same artifact.
Missing, mismatched, stale, or incompatible manifests fail selection. No keyword
or broad-only replacement is used. Changed tests and previous failures remain
mandatory even when they exceed the temporary ceiling; the manifest reports the
overflow explicitly.

The query admits regions no wider than 40 lines with at most 150 distinct test
owners. These are conservative initial limits, not a validated recall claim.
The final ceiling is 250 tests and the operational target remains below 100.
`SelectionConfig` is shared by queries, scoring, diagnostics, monitoring, and
replay. Change the selector version when changing the persisted contract.

## Coverage publication

Coverage collection and export preserve recorded file paths. The selector
normalizes diff paths and explicitly queries both bare and dotted spellings,
preserving filtering by `file`. It combines their observations under one
repository-relative path before scoring. Absolute paths and parent traversal
are rejected when interpreting selection inputs. Generated protobuf coverage
under `ci/tmp/build/` remains in the export; selection only considers changed
source paths under `src/`.

Each shard publishes `coverage-export-N.json`, including workflow run ID, SHA,
shard, executed/exported test inventories, randomized-settings fingerprints, and
post-export selector smoke status and its path interpretation version. Failed
tests retain their coverage. Missing attribution for an armed test fails the
export check. The final `Coverage health`
job requires all eight exports to come from the same workflow run.

CIDB still has the legacy eight-column `checks_coverage_lines` schema. Until an
external migration adds durable workflow/shard identity to coverage rows, the
selector uses the latest three usable `(check_start_time, check_name)` snapshots
per shard, searching 14 days and requiring a snapshot within 72 hours for every
shard. At least 100 exported tests are required for a usable shard snapshot.
These temporary keys are **not** workflow-run IDs; shards can start in different
hours. New exports retain second-resolution timestamps. The JSON sidecars carry
durable run identity, but selecting complete workflow runs directly in CIDB
remains dependent on that schema migration. Partial rows remain available even
when the final workflow health check fails.

## Validation and rollout

Run deterministic smoke without network access:

```bash
python3 -m ci.jobs.scripts.test_selection_smoke
```

Operational monitoring uses the production query and scorer:

```bash
python3 -m ci.jobs.scripts.test_selection_smoke --live
```

The production entry-count feature is disabled. `min_depth` is an LLVM function
entry count, not call depth: 254 is censored and 255 unavailable. Shadow manifests
compare the legacy low-count tier with bounded region-relative low/high-count
bonuses. All scorers consume the same deduplicated observations.

Replay JSONL cases with pre-PR snapshots and independently sourced labels:

```bash
python3 -m ci.jobs.scripts.evaluate_test_selection tmp/cases.jsonl \
    --output tmp/replay.json
```

The module docstring describes the input contract. `--query-url` fetches features
through the production query at each case's cutoff. Future observations and
unhealthy snapshots are errors. Changed regression tests are reported separately
and do not establish coverage recall. A review-ready dataset needs at least 60
days, actual failures, later flaky fixes, linked regressions, and controls.

`expanded_targeted_matrix` remains disabled pending that replay and shadow review.
The generated matrix uses the regular PR functional configurations plus the
original ARM ASan job. Dedicated Azure, LLVM coverage, and excluded-from-LLVM job
groups are outside this matrix. LLVM coverage modes in the regular configurations remain
exempt because those runners disable randomized settings. The selected sanitizer
configurations are derived from the full-suite definitions. Targeted repetitions preserve each
configuration's runner, build, environment, timeout, and flavor.

`selection-execution.json` accounts for every base-selected test, including flavor
and runner filters, starts, completions, outcomes, repetitions by settings
fingerprint, and early-stop reasons. The entire compatible selection is repeated;
ordinary selected jobs remain one-shot.

Validation on 2026-09-05 passed the live production canary with fresh snapshots
from all eight shards. A pre-PR replay attempt for
https://github.com/ClickHouse/ClickHouse/pull/117331 was rejected because its newest
coverage was from 2026-08-27. The earlier exploratory ranking example used newer
coverage and cannot establish historical recall. Neither the 60–90-day quality
gate nor per-configuration repetition budgets have been validated yet.
