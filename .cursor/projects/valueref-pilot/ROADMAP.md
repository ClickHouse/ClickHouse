# Roadmap

Phased plan from proof-of-concept to (possible) broader rollout. Each phase is a gate: do not
start the next phase until the current one's exit criteria are met and recorded in
`PROGRESS_LOG.md`. Granular tasks live in `TASKS.md`.

## Phase 0 — Infrastructure (this phase)

Set up persistent project memory so any agent can resume from either environment.

Exit criteria:
- [x] Project memory directory created and committed.
- [x] Claude Code discovery hook (skill) in place.
- [ ] Branch pushed and draft PR opened.

## Phase 1 — Introduce `ValueRef` (foundation)

Add the `ValueRef` type and a unit test, with **no call-site migrations yet**. Keep it a pure,
self-contained addition that compiles and is covered by tests.

Scope:
- Add `ValueRef` (header, likely `src/Columns/ValueRef.h` or `src/Core/`), per `DESIGN.md`.
- Add `src/Core/tests/gtest_value_ref.cpp` (or under `src/Columns/tests/`) proving: construction,
  `isValid`, `toField` round-trip equals `operator[]`, and `compareAt`-based comparison matches
  `Field` comparison across representative column types (number, string, nullable, array, const,
  low-cardinality).

Exit criteria:
- Builds; unit test passes; no behavior change anywhere else.
- `sizeof(ValueRef)` confirmed small (via `.claude/tools/cppexpr.sh`).

## Phase 2 — First migration (measurable pilot)

Migrate exactly **one** hot call site (recommended: `SingleValueDataGeneric` aggregate state, or
`KeyCondition` min/max checking — see `DESIGN.md` "Where it plugs in"). Choose the lower-risk of
the two first if uncertain.

Scope:
- Route the chosen call site through `ValueRef`/column primitives instead of `Field`.
- Add/extend targeted tests. Ensure existing stateless + gtest suites pass.
- Build a microbenchmark and/or use an existing perf test to measure before/after.

Exit criteria:
- Behavior identical (tests green, incl. a fuzz/consistency test vs the `Field` path).
- Perf: no regression; ideally a measurable improvement. Numbers recorded in `PROGRESS_LOG.md` and
  the PR.

## Phase 3 — Evaluate & decide

With one real migration and numbers in hand, decide whether the approach generalizes.

Scope:
- Write an assessment in `PROGRESS_LOG.md`: measured wins, friction, correctness hazards found.
- Update `DESIGN.md` with lessons.
- Produce a prioritized list (in `TASKS.md`) of the next N call sites, ranked by
  (hotness × ease × safety).

Exit criteria:
- A go/no-go recommendation for broader rollout, backed by data.

## Phase 4+ — Broader rollout (only if Phase 3 says go)

Incrementally migrate additional hot call sites, one reviewable PR each. Candidate areas (all
column-backed, none touching formats): remaining generic aggregate states, `IColumn` extremes/
min-max helpers, `Range`/`FieldRef` internals, and reducing `getValue<T>()`/`getField()` usage in
functions.

Non-goals for every phase: removing `Field`; changing SQL-literal, settings, or on-disk/wire
representations. See `AGENT_GUIDE.md` §6.
