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
