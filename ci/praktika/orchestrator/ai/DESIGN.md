# AI orchestration — design

This package is the plumbing skeleton for letting an LLM observe a running
workflow and (eventually) decide what to do — skip/cancel/reorder jobs,
auto-fix failures, triage review findings (see ClickHouse/ClickHouse#101924).

**Current scope is intentionally minimal:** the orchestrator drives an
*advisor* through the run lifecycle; the advisor routes each event to the
matching hook on a pluggable model provider, and any turn it produces is
recorded. Real work happens on **failure** only — `anthropic` / `bedrock` (plus
a `mock` that returns a no-op turn). The goal is to lock in the interfaces and
the flow so real model SDKs, action dispatch, and cost accounting slot in later
without rewriting the orchestrator loop.

## Flow

```
orchestrator loop (_orchestrate_single)
  ├─ advisor.on_run_start(state, event) # → provider.on_run_start (no-op today)
  └─ while state.not_finished():
       kick ready jobs
       state.wait()                     # blocks; sweeps S3 → a job result lands
       advisor.on_workflow_update(state, event)
            ├─ _delta(state)            # jobs newly terminal since last turn
            │     └─ (none) → return    # idle ticks route nothing
            ├─ failures  → provider.on_job_failure(obs) → Turn(...)  # real model
            ├─ successes → provider.on_job_success(obs)              # no-op today
            │   (skipped / cancelled tracked, but routed to no hook)
            └─ per recorded turn: ledger.add + trace.record + dispatch
       patch top-level check
  └─ finally: advisor.finalize()        # → provider.on_run_finish + run cost total
```

Routing keys off jobs that **newly reach a terminal state** (an actual "result
received", not a poll tick), then dispatches each to its own provider hook. A
hook that opts out (returns `None` — the default for every event but
`on_job_failure` today) makes no model call and records no turn, so a green run
costs nothing. Skipped/cancelled transitions are tracked so they don't re-fire
but route to no hook: they carry no problem to act on, and re-consulting on a
cascade cancel would just burn tokens.

## Module layout

| File | Responsibility |
|---|---|
| `provider.py` | The stable contract: `AIProvider` ABC (event hooks) + `Observation` / `Turn` / `Usage` dataclasses + a name→class registry (`register`, `resolve`). |
| `mock.py` | `MockProvider` — implements `on_job_failure` with a no-op `Turn`, zero usage, no network. |
| `trace.py` | `TraceLogger` (stdout + `TEMP_DIR/ai/turns.jsonl`) and `UsageLedger` (run totals — the cost seam). |
| `__init__.py` | `OrchestratorAI` (delta detection, event routing, records turns) + `build_observation` + `OrchestratorAI.maybe_create` factory. |

## The provider contract

`Observation` (in) and `Turn` (out) are plain, JSON-serializable dataclasses —
the boundary the orchestrator depends on. The contract is **event-typed**: the
advisor calls a hook named for the lifecycle event, and every hook is a no-op
by default (`return None`), so a provider implements only the events it reacts
to:

| Hook | Fires when | Wired to a model today |
|---|---|---|
| `on_run_start(obs)` | the run starts, before any job is terminal | no |
| `on_job_failure(obs)` | one or more jobs failed this turn | **yes** |
| `on_job_success(obs)` | one or more jobs passed this turn | no |
| `on_run_finish(obs)` | the run reached a terminal state | no |

A hook that consults a model:

1. formats the `Observation` into a prompt + tool specs,
2. runs the SDK's tool-use loop,
3. fills `Usage` from the SDK's token report and computes `cost_usd` from a
   pricing table,
4. returns a `Turn` with `reasoning` and (later) a populated `decision`;
   or returns `None` to opt out for this event.

None of that touches `praktika/orchestrator/__init__.py`. The advisor never
calls a hook directly — it goes through `AIProvider.consult(event, obs)`, which
brackets the hook with observation/turn tracking (feeding the **AI Advisor**
GitHub check: in-progress while an observation is out, neutral once the turn
lands) and turns a provider exception into an error `Turn`, so a provider bug
never crashes the run. Hooks stay pure model logic; an unimplemented hook is a
no-op that consult skips (no tracking, no check).

### Adding a provider

```python
# praktika/orchestrator/ai/anthropic.py
from .provider import AIProvider, Turn, Usage

class AnthropicProvider(AIProvider):
    name = "anthropic"
    def on_job_failure(self, observation) -> Turn:
        ...  # call SDK, fill Usage, return Turn
    # on_run_start / on_job_success / on_run_finish inherit the no-op default
```

Register it in `provider.py` (next to the mock) and set
`Workflow.Config.ai_orchestrator.provider = "anthropic"` on the workflow that
should use it.

## Configuration

`praktika/workflow.py` (`Workflow.OrchestratorAI.Config`, defaults shown below):

| Setting | Default | Meaning |
|---|---|---|
| `enabled` | `False` | Master switch. When off, `maybe_create` returns `None` and the loop is unchanged. |
| `provider` | `"mock"` | Registered provider name. If it can't be resolved or instantiated by the running runtime (an older orchestrator that predates the provider, or a provider whose SDK isn't installed), `maybe_create` logs and returns `None` — the advisor is disabled, never a crash. |
| `model` | `""` | Provider-specific model id; empty = provider default. |

The advisor is advisory and best-effort: neither a disabled switch, an
unresolvable provider, nor a session-setup error may break core orchestration.

This repo enables AI per workflow via `Workflow.Config.ai_orchestrator`. Because the
orchestrator runs the *published* runtime wheel (not the PR's praktika code), a
runtime that predates a provider just disables the advisor for that run rather
than failing CI — publish a runtime that registers the provider before relying
on it.

## Bedrock operational notes

The `bedrock` provider uses the EC2 instance role of the workflow orchestrator
pool and calls Bedrock Runtime directly. In steady state the role only needs
`bedrock:InvokeModel`.

There is one important first-use caveat for Anthropic models on Bedrock:

- Bedrock may require a one-time model-access / Marketplace agreement per AWS
  account, region, and model family or inference profile.
- If that agreement has not been accepted yet, an invocation can fail with an
  access-denied error mentioning `aws-marketplace:ViewSubscriptions` or
  `aws-marketplace:Subscribe`.
- We do **not** grant those Marketplace actions to the orchestrator role by
  default. Instead, enable the model once in the target account manually, then
  leave the runtime role limited to `bedrock:InvokeModel`.

If AI orchestration is being enabled in a fresh account or for a newly adopted
Bedrock model, verify the agreement has been accepted before expecting the
orchestrator to use it.

## Tracing & cost

- **Per turn:** a one-liner to stdout (live in `journalctl -fu
  praktika-controller`) and a full JSON record appended to
  `TEMP_DIR/ai/turns.jsonl` — `{turn, ts, changed, observation, reasoning,
  decision, usage, error, ledger}`. Enough to replay the AI's exact view and
  decision after the fact.
- **Per run:** `UsageLedger` totals turns / input+output tokens / `cost_usd`,
  printed at `finalize()`.

## Investigation tools

The `anthropic` / `bedrock` providers run a tool-use loop (`on_job_failure` in
`anthropic.py`) so the model can gather evidence before deciding. The hook only
fires on a failure, so the repo-read tools are always offered (and `fetch_log`
whenever the observation carries log links); the loop is bounded by
`_MAX_TOOL_ROUNDS`:

| Tool | Purpose | Guard |
|---|---|---|
| `fetch_log` | Read a job log/artifact (optionally grepped) | URL must be in the observation's `links` (SSRF allowlist); size-capped |
| `grep_repo` | `git grep` the checked-out PR to locate code | Rooted at the repo; result-capped |
| `read_file` | Read PR source by repo-relative path | Path must resolve inside the repo root; size/line-capped |

Token usage is summed across every round-trip and the model's `root_cause` is
folded into `Turn.reasoning` for the trace.

## Action dispatch

`OrchestratorAI._dispatch` applies the first actionable decision after a turn is
recorded. An error `Turn` never dispatches, and the provider's decision is
authoritative — there is no separate enable flag. Wired today:

- **`cancel_run`** — sets `state.cancelled`; the orchestrator loop cancels
  unfinished jobs + writes the S3 cancel flag on its next iteration.
- **`cancel_and_patch`** — the model attaches `edits: [{path, search, replace}]`
  (exact, once-only text replacements against files it read via `read_file`).
  `_apply_edits` validates all edits and applies them atomically to the checked-
  out PR (path-guarded, all-or-nothing), then an **injected patcher** commits +
  pushes them to the PR branch (triggering a fresh run), `session.record_edit`
  logs the diff + commit, and `state.cancelled` tears the superseded run down.
  The patcher is supplied by the orchestrator (`_make_ai_patcher`, closed over
  `gh_token`/`repo`/`head_ref`); it is **same-repo only** — an `ls-remote` guard
  requires the branch to exist on the base repo at `head_sha`, which rejects fork
  PRs and human-takeover, and the push is never forced. When there is no patcher
  (local mode / fork / no token), the round budget is spent
  (`can_continue_round`), or the edits don't apply/push cleanly, the decision
  stays advisory (recorded, run left to finish normally).

## Non-goals (next phases)

- **Fork-PR patching** — `cancel_and_patch` is same-repo only for now.
- **S3 persistence** of traces (currently local + stdout only).
- **Pricing tables** beyond the models in `anthropic.py`'s `_PRICING`.
