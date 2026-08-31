# AI sessions — managing the log across CI runs

An AI fix-loop does not fit inside one CI run. The orchestrator process is
**ephemeral and per-sha** — every push is a new SQS message, a new instance, a
new `run_id`. But when the AI edits code and pushes, that triggers *another* CI
run, and the AI needs to remember what it already tried. `SessionManager` is the
durable layer that spans those runs.

## Concepts

```
PR     (pr key)         session.json          rounds/runs index, cumulative cost, budget
 └ Round (round_id)      rounds/<id>.json      goal, status, run_ids, edits, cost
    └ Run  (run_id/sha)  runs/<id>/run.json    event, job outcomes, cost
       └ Turn            runs/<id>/turns/…      one advisor consultation (a result landed)
          └ Edit         runs/<id>/edits/…      diff the AI produced + resulting commit
```

- **PR log** — the session manifest plus every round and run. (`pr_log()`)
- **Round log** — one AI problem-solving episode: edit → push → rerun → observe
  → edit … across many CI runs, until a run goes green (resolved), budget runs
  out (abandoned), or a human pushes (superseded). (`round_log(round_id)`)
- **Turn** — what the `Advisor` produces when it consults the provider (today,
  when a job fails).

## Round boundaries are implicit

There is no "open a round" action the model has to call. A round opens
**automatically** the first time a run reports a failure with no round already
open, and closes automatically when a later run in that round goes green. The
goal string and trigger are derived from the failing jobs.

Continuity across the per-sha orchestrators is recovered from the persisted
`session.json` (`open_round_id`): when a fresh orchestrator boots for the next
sha, `begin_run` reads the session and rejoins the open round. A future refinement
distinguishes "the AI's own push continues the round" from "a human pushed, so
the AI steps back" using the commit author / an `AI-Session-Round:` commit
trailer — the manifest is the source of truth until then.

## Storage: blobs + index

| Layer | What | Backend |
|---|---|---|
| **Blob store** (`SessionStore`) | manifests (JSON), turn streams (append-only events), edit patches | **S3** in CI (`S3SessionStore`), local fs in local mode / tests (`LocalSessionStore`) |
| **Index** (`index`) | one queryable row per turn / round — provider, model, tokens, cost, decision types, outcome | `LoggingIndex` stub now; **CIDB (ClickHouse)** later, reusing `praktika/cidb.py` |

This is the classic split: S3 holds the canonical, replayable log; the index
makes cost and decision-flow queryable across PRs (the original motivation —
track reasoning, cost, flow). Turn streams are append-only — JSONL locally, one
zero-padded object per event on S3 (which has no append), reassembled in order
on read.

### Key layout

```
ai-sessions/pr/<pr>/
  session.json
  rounds/<round_id>.json
  runs/<run_id>/run.json
  runs/<run_id>/turns/<seq>.json        # or turns.jsonl locally
  runs/<run_id>/edits/<edit_id>.patch
```

## What we keep, and why compact

We store the AI's *view and decisions*, not raw artifacts: the observation it
was given, its reasoning, the decision/tool-calls, usage, and any patch it
produced (paired with the commit that carried it). Raw job logs stay in their
existing S3/report locations — the session keeps pointers and the failure
snippet the AI actually consumed.

This compactness matters because **the round log is also the AI's memory**.
Before the AI decides on a rerun it is a brand-new process; `round_context_for_prompt(round_id)`
assembles a small summary — the goal, the diffs already tried, and the last
reasoning per attempt — to prepend to the next prompt so it does not repeat a
fix that already failed. Storing structured manifests (not giant dumps) is what
makes that summary cheap to build and cheap to put in context.

## SessionManager API

```python
SessionManager.from_event(event, run_id, local_mode)  # pick store, load/create session
  .begin_run(run_id, sha, event)        # register run; rejoin open round if any
  .observe_turn(observation, turn)      # implicit round-open; append; roll up cost; console+index
  .record_edit(patch, commit_sha, files)# AI edit seam (unused by mock)
  .finalize_run(conclusion, outcomes)   # close run; auto-resolve round on green
  .close_round(status, outcome)
  .can_continue_round()                 # budget gate (simple stub)
  # fetch
  .pr_log(); .round_log(round_id)
  .round_context_for_prompt(round_id)   # AI memory for the next run
  .cost_summary()                       # per-PR + per-round
```

`OrchestratorAI` (per run) calls `begin_run` once at creation, `observe_turn` for each
recorded turn (today, on a job failure), and `finalize_run` at the end. The advisor never touches the
store or the index directly.

## Configuration

| Setting | Default | Meaning |
|---|---|---|
| `pr_token_cap` | `0` | Per-PR cumulative token cap (input + output, 0 = off). |
| `max_rounds` | `0` | Max CI-run iterations per round (0 = off). |

## Status / non-goals

- Round lifecycle, persistence, fetch, and cost roll-ups are implemented and
  exercised end-to-end by the **mock** provider (which makes no real decision
  but produces a structured, non-actionable one so the flow is visible).
- The **index** is a logging stub — the CIDB writer is the next step.
- `record_edit` is now written by the `cancel_and_patch` dispatch (the advisor
  applies the model's edits, commits+pushes, and logs the diff+commit here); the
  no-op mock still never calls it.
- Budget checks are simple stubs.
- AI-authored-push detection (human-takeover) is manifest-based for now.
