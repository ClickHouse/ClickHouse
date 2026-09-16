# Precise stateless selection

`Select functional tests` produces a single manifest for a PR SHA, selector
version, workflow run, and attempt. A conditional S3 insert makes the first
successful manifest immutable within that attempt. All targeted configurations
consume it; a new attempt refreshes previous failures and coverage snapshots.
If only failed jobs are rerun, the targeted jobs create or retrieve the manifest
for the new attempt through the same producer.
Missing, mismatched, stale, or incompatible manifests fail selection. No keyword
or broad-only replacement is used. Changed tests and previous failures remain
mandatory even when they exceed the temporary ceiling; the manifest reports the
overflow explicitly. Changes to CI scripts do not add a fixed smoke-test list
to the selection. Deleted and renamed fixtures select their surviving owning tests.

PRs without eligible changed source lines do not query or validate coverage;
changed tests and previous failures still populate the manifest. When coverage
is needed, the canary chooses a qualifying source region from the current
snapshots and exercises the production query and scorer against it.

The query admits regions no wider than 40 lines with at most 150 distinct test
owners. These are conservative initial limits, not a validated recall claim.
The final ceiling is 250 tests and the operational target remains below 100.
`SelectionConfig` is shared by queries, scoring, diagnostics, monitoring, and
replay. Change the selector version when changing the persisted contract.

The manifest records commit and selector identity, configuration, coverage
snapshots, source diff coordinates, and an ordered `tests` list. Each test has
its selection reasons, score, and compact coverage evidence: matching regions,
owner counts, changed lines or hunks, and observation counts. Rejected candidates
retain their scores, evidence, and rejection reason; mandatory overflow is
reported explicitly. Test names are stored once. Full SQL queries, unrelated
diff lines, duplicate candidate lists, and experimental scoring diagnostics are
omitted from the manifest.

## Coverage publication

Coverage collection and export preserve recorded file paths. The selector
normalizes diff paths and explicitly queries both bare and dotted spellings,
preserving filtering by `file`. It combines their observations under one
repository-relative path before scoring. Absolute paths and parent traversal
are rejected when interpreting selection inputs. Generated protobuf coverage
under `ci/tmp/build/` remains in the export; selection only considers changed
source paths under `src/`.

Each shard runs a post-export smoke check through the production selector query
and scorer against its uploaded coverage. The export step fails if the selector
cannot retrieve and score a precise source region.

CIDB still has the legacy eight-column `checks_coverage_lines` schema. Until an
external migration adds durable workflow/shard identity to coverage rows, the
selector uses the latest three usable `(check_start_time, check_name)` snapshots
per shard, searching 14 days and requiring a snapshot within 72 hours for every
shard. At least 100 exported tests are required for a usable shard snapshot.
These temporary keys are **not** workflow-run IDs; shards can start in different
hours. New exports retain second-resolution timestamps. Selecting complete
workflow runs directly in CIDB remains dependent on that schema migration.

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

PRs run targeted checks in three configurations: AMD ASan with database disk
and distributed plan, AMD TSan with S3 storage, and ARM ASan. Each job repeats
the complete related test list up to 50 times with randomized settings and a
30-minute budget with setup time deducted. The runner stops gracefully, allowing
in-flight tests and cleanup to finish. The failure limit can stop execution earlier.
Targeted jobs run only in the PR workflow. Master continues to run the full
functional suite.

`expanded_targeted_matrix` remains disabled pending replay and shadow review.
It adds targeted checks for the other regular PR functional configurations.
Dedicated Azure, LLVM coverage, and excluded-from-LLVM job groups are outside
this matrix. LLVM coverage modes in the regular configurations remain exempt
because those runners disable randomized settings. Targeted configurations
are derived from the full-suite definitions. The runner schedules parallel and
sequential tests within one job, with 50 repetitions for both. When combining
execution flavors, the job uses the parallel flavor's runner. Build, storage,
and query settings still define separate configurations.

Validation on 2026-09-05 passed the live production canary with fresh snapshots
from all eight shards. A pre-PR replay attempt for
https://github.com/ClickHouse/ClickHouse/pull/117331 was rejected because its newest
coverage was from 2026-08-27. The earlier exploratory ranking example used newer
coverage and cannot establish historical recall. Neither the 60–90-day quality
gate nor per-configuration repetition budgets have been validated yet.
