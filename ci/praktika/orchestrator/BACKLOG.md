# Orchestrator Backlog

Design issues worth revisiting during a larger refactor. Each entry: the
problem, why the current code works around it, and the direction to consider.

---

## Re-run: intent vs. work-set mismatch, non-atomic reset

**Problem.** A re-run request (`runs/<run_id>/rerun-request/*`) records the
*root* jobs the user asked for. But `apply_rerun` re-runs the transitive
FAILED/CANCELLED closure — a strictly larger set computed at execution time and
never persisted. `_reset_job` mutates state per job (delete `final.json`, flip
PENDING, `rerun_count++`, repost check) with no all-or-nothing boundary, so a
closure can be reset partially: root `A` succeeds while downstream `B` fails.

Consequences seen:
- Consumption keyed to the request (roots) can't account for failures on
  derived nodes (`B`), so a request gets deleted with work still undone.
- Replaying the original request doesn't recover it — `A` is now PENDING, so
  the closure walk no longer reaches `B`. The request is not idempotent against
  the state it mutates.

**Status.** Open — `sweep_rerun` retains a request only when a *root* it names
failed to reset, so a failure on a derived node (`B`) still drops the work.

**Direction to consider.** Reframe reset as **prepare → commit**:
1. Prepare: clear all S3 objects for the whole closure (stop at first failure).
2. Commit: only if prepare fully succeeded, flip all in-memory state, consume
   the request, `save_snapshot()` (the durable commit point).
3. On prepare failure: drop the re-run, record the error in state, and surface
   it in the report (`Result.ext.errors`) instead of retrying.

This makes reset atomic-in-intent, makes request consumption unconditionally
safe, and removes the retry marker. Caveats: S3 delete isn't transactional, so a
mid-closure failure can't be rolled back (stop-at-first-failure keeps the common
permission/config case clean); and drop-on-failure trades transient resilience
for immediate visibility (acceptable — post-SDK-retry failures here are almost
always permanent).

---

## Report: finish making the orchestrator the sole summary writer, then drop result versioning

**Problem.** On the native path the workflow report summary
(`PRs/<pr>/<sha>/<workflow>/…json`) is a single mutable object with several
concurrent writers: the orchestrator (`create_initial_report` + `publish_report`
each loop) *and* every job's runner (`hook_html.pre_run`, `post_run`) *and* the
Config job (`hook_html.configure`). Each does a whole-object read → modify →
re-upload via `_ResultS3.update_workflow_results`. Because more than one writer
touches the same object, the writes must be serialized by optimistic locking —
`copy_result_*_with_version`'s version CAS + the `MAX_ATTEMPTS` retry loop —
or concurrent RMWs lose each other's rows.

Creation ownership already moved to the orchestrator (Config's
`push_pending_ci_report` no-ops under `ORCHESTRATOR_OWNS_REPORT`; the orchestrator
does the sole `version=0` create). But the *row/message* writers on the runner
side did not, so versioning is still load-bearing.

**Status.** Open — version CAS is still required because `pre_run`/`post_run`
still write the summary. Item 3 below (`configure`) is **done**: the orchestrator
now authors the cached/filtered SKIPPED rows (`apply_workflow_config` stashes the
cache link, `publish_report` writes the rows), and `configure` no-ops on the
native path — one fewer concurrent writer (see REPORT_OWNERSHIP.md increment 4).
`post_run`'s `update_workflow_results` still can't be gated off "without first
splitting usage aggregation out (a larger change)."

**Direction to consider.** Stand the runner-side summary writers down on the
native path so the orchestrator is the *only* writer, then drop the CAS:
1. `post_run` — skip the summary `update_workflow_results` on native. The
   orchestrator already re-asserts each row from `final.json`; move the two things
   `post_run` still contributes into it: **report messages** (warnings/errors) and
   **DROPPED-dependee rows** (computed on a blocking job failure).
2. `pre_run` — move its stale-report-message clear into the orchestrator's reset
   path.
3. `configure` — **done.** The orchestrator authors the cached/filtered SKIPPED
   rows (`apply_workflow_config` + `publish_report`); `configure` no-ops on native.
4. Runners keep writing only their own `result_<job>.json`; nothing but the
   orchestrator mutates the summary object.

Once no runner touches the summary, the single writer makes the version CAS
unnecessary — every summary write becomes an unconditional `version=0` (or a
plain PUT), which is simpler and faster. This is the natural continuation of the
creation-ownership move: create → rows/messages → versioning falls away.
