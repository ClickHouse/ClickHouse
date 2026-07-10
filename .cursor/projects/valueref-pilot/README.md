# ValueRef Pilot — reducing reliance on `Field` in the ClickHouse server

This directory is the **persistent memory and control center** for a long-running engineering
project. Its purpose is to let any agent (running from the Cursor cloud UI, or from a Linux
console via the API) pick up the work with full context, without re-deriving anything.

If you are an agent starting a session on this project, **read [`AGENT_GUIDE.md`](./AGENT_GUIDE.md)
first**, then skim the current state in [`TASKS.md`](./TASKS.md) and the latest entry in
[`PROGRESS_LOG.md`](./PROGRESS_LOG.md).

## The one-paragraph summary

`DB::Field` (`src/Core/Field.h`) is a hand-rolled discriminated union representing a single
database value. It has two structural problems: (1) it is a *value carrier* that in most cases
could be replaced by a reference into an existing column row, and (2) its type tags do **not**
correspond 1:1 to ClickHouse SQL types (e.g. `UInt8`, `Date`, `Enum8`, `bool` all collapse to
`UInt64`; `Float32` collapses to `Float64`). The project introduces a lightweight **`ValueRef`**
(a `const IColumn* + row index`, carrying no owned value) and migrates hot, `Field`-materializing
call sites to it, starting with a measurable pilot. Full removal of `Field` is **out of scope**
and considered infeasible; **reduction** on hot paths is the goal.

## What lives here

| File | Purpose | Update cadence |
|------|---------|----------------|
| [`README.md`](./README.md) | This orientation page. | Rarely. |
| [`AGENT_GUIDE.md`](./AGENT_GUIDE.md) | Operating manual for agents: workflow, git/PR conventions, build & test commands, how to update this memory. | When process changes. |
| [`INVESTIGATION.md`](./INVESTIGATION.md) | The "why": findings from the initial codebase investigation, with file:line references. | Append as new facts are learned. |
| [`DESIGN.md`](./DESIGN.md) | The "what": the `ValueRef` design proposal and API sketch. | Revise as the design evolves. |
| [`ROADMAP.md`](./ROADMAP.md) | Phased milestones from pilot to broader rollout. | At phase boundaries. |
| [`TASKS.md`](./TASKS.md) | Granular, checkable task backlog and current status. | Every session. |
| [`PROGRESS_LOG.md`](./PROGRESS_LOG.md) | Append-only journal: what was done, decisions, benchmark numbers, links to PRs/CI. | Every session. |

## How to work on this from either environment

The project memory here is **tool-neutral** — plain git-tracked markdown that any agent can read.
Only the *discovery hook* differs per tool:

- **From the Cursor cloud UI:** start an agent and point it at this directory (or just mention the
  "ValueRef pilot"); it follows `AGENT_GUIDE.md`.
- **From a Linux console with Claude Code:** the project is registered as a Claude skill at
  `.claude/skills/valueref-pilot/SKILL.md`, which Claude auto-discovers. Ask Claude to "continue
  the ValueRef pilot" (or mention `Field`/`ValueRef` work) and it will load that skill and route
  itself into this directory. You can also just tell it to read
  `.cursor/projects/valueref-pilot/AGENT_GUIDE.md`.
- **From any console / API:** `git fetch` and check out the project branch (or `master` once
  merged), `cd` to the repo, and read this directory. All state is in git; nothing lives only in
  an agent's ephemeral context.

## Ground rules (see `AGENT_GUIDE.md` for detail)

- Follow the repo's `AGENTS.md` rules (ClickHouse contributor and agent conventions).
- Never commit to `master`; every unit of work goes on a `cursor/<name>-f766` branch with a PR
  targeting `master`.
- Keep this memory current: update `TASKS.md` and append to `PROGRESS_LOG.md` **before ending a
  session**, so the next agent is never lost.
