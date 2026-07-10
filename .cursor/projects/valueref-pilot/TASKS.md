# Tasks

**Current focus:** Phase 0 — stand up project infrastructure. Next actionable work is Phase 1
(introduce the `ValueRef` type + unit test).

Status legend: `TODO` · `IN PROGRESS` · `DONE` · `BLOCKED` · `DROPPED`.
Keep this file current every session (see `AGENT_GUIDE.md` §3). When you start a task, set it
`IN PROGRESS` and put your branch/PR next to it.

## Phase 0 — Infrastructure

- [x] `DONE` Investigate `Field` usage and feasibility (see `INVESTIGATION.md`).
- [x] `DONE` Create persistent project memory under `.cursor/projects/valueref-pilot/`.
- [x] `DONE` Add Claude Code discovery skill (`.claude/skills/valueref-pilot/SKILL.md`).
- [ ] `IN PROGRESS` Push branch `cursor/valueref-pilot-infrastructure-f766` and open draft PR.

## Phase 1 — Introduce `ValueRef`

- [ ] `TODO` Decide final location/namespace for `ValueRef` (candidates: `src/Columns/ValueRef.h`,
  `src/Core/ValueRef.h`). Record decision in `PROGRESS_LOG.md`.
- [ ] `TODO` Implement `ValueRef` per `DESIGN.md` (non-owning `{const IColumn*, size_t row}`,
  `isValid`, `toField`/`toField(Field&)`).
- [ ] `TODO` Add comparison helper(s) built on `IColumn::compareAt`; define structural-equality
  precondition and null/NaN handling.
- [ ] `TODO` Add `gtest_value_ref.cpp`: round-trip vs `operator[]`, and comparison parity vs
  `Field` across number/string/nullable/array/const/low-cardinality columns.
- [ ] `TODO` Confirm `sizeof(ValueRef)` via `.claude/tools/cppexpr.sh` and note it in the log.
- [ ] `TODO` Build `unit_tests_dbms`, run `*ValueRef*`, summarize log via subagent.

## Phase 2 — First migration (pick ONE)

- [ ] `TODO` Choose pilot call site: `SingleValueDataGeneric` (lower risk) **or** `KeyCondition`
  min/max checking (higher value). Record rationale.
- [ ] `TODO` Implement the migration behind the chosen call site; keep `Field` path intact where
  it is still the correct boundary (query constants, `±∞`).
- [ ] `TODO` Add a consistency test comparing `ValueRef` path vs `Field` path results.
- [ ] `TODO` Build a microbenchmark and/or select an existing perf test; capture before/after
  numbers.
- [ ] `TODO` Run relevant stateless tests; summarize logs via subagent.

## Phase 3 — Evaluate

- [ ] `TODO` Write go/no-go assessment with data in `PROGRESS_LOG.md`; update `DESIGN.md`.
- [ ] `TODO` Rank next candidate call sites by hotness × ease × safety.

## Backlog / ideas (unscheduled)

- [ ] `TODO` Audit and reduce `ColumnConst::getValue<T>()` / `getField()` call sites in
  `src/Functions/` that construct a `Field` just to read one scalar.
- [ ] `TODO` Consider a column-native `getExtremes` variant that avoids `Field` min/max.
- [ ] `TODO` Explore composing `FieldRef` on top of `ValueRef` (see `DESIGN.md` open question).

## Discovered issues / notes

_(Append findings here as tasks surface. Include file:line and a one-line description.)_
