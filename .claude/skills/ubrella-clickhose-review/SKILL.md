---
name: ubrella-clickhose-review
description: Use when the user asks for a multi-perspective review of a ClickHouse / C++ diff, PR, branch, commit range, or commit hash. Triggers on requests like "review this PR", "umbrella review", "ubrella review", "do a full review", "deep review of branch X", or any ClickHouse code review where multiple independent angles (security, perf, concurrency, lifetime, compat, tests, etc.) should be considered before producing a consolidated report.
---

# Umbrella ClickHouse Review

Multi-perspective code review for ClickHouse / C++ diffs. Prepare neutral shared context, dispatch specialized review subagents in parallel, validate findings, and produce one consolidated high-signal report.

## When to Use

- User provides a PR number/URL, branch name, commit hash, commit range, or explicit diff spec and wants a thorough review.
- Change touches non-trivial ClickHouse code (server, storage, parsers, network, Keeper, formats, settings, SQL surface).
- User explicitly invokes "umbrella review" / "ubrella review".

**Don't use for:** trivial doc-only changes, single-line fixes, or when user asks for a quick scan only.

## Workflow

### 1. Resolve the diff

Determine input type and gather material:

- **PR (number or URL):** `gh pr view <N> --json title,body,baseRefName,headRefName,files,commits` and `gh pr diff <N>`.
- **Branch:** `git diff <base>...<branch>` and `git log <base>..<branch>`. Default base is `master` unless specified.
- **Commit range / hash:** `git diff <range>`, `git log <range>`.
- **Explicit diff spec:** use as given.

Record: base ref, head ref, file count, commit messages, PR title/description.

### 2. Build neutral shared context

Before launching reviewers, prepare a single context block. **Facts only — no judgement.** Include:

**File / subsystem map:**
- changed files grouped by subsystem/component
- touched public APIs / settings / config / formats / protocols
- touched tests, docs, build files, scripts, generated files
- likely hot paths
- likely user-facing behavior
- new or changed shared state
- new or changed external inputs / SQL-visible parameters / file paths
- removed or relaxed tests
- large or binary files

**Behavior map (when applicable):**
- user/system entrypoints
- validation and dispatch layers
- state / storage / cache interactions
- downstream integrations: filesystem, network, Keeper/ZooKeeper, background workers, services
- exception and error-propagation paths
- state transitions and side effects
- important invariants the change appears to rely on

Only include facts visible from the diff, commit messages, PR metadata, and nearby code.

### 3. Dispatch subagents in parallel

Use the `Agent` tool with `subagent_type: "general-purpose"` and `model: "sonnet"` (cheaper but capable). Send all applicable subagents in a **single message with multiple Agent tool calls** so they run concurrently.

Each subagent gets:
- the neutral shared context (verbatim)
- the diff
- its assigned scope and prompt (below)
- the **General review rules** and **Required output format** (below)

Skip subagents that clearly do not apply (e.g. concurrency review on a docs-only change). Always run subagents 1, 2, 3, 7, 11, 12. Run 15 (deep audit) only for complex / high-risk changes.

### 4. Aggregate

- Deduplicate findings across subagents.
- Verify file/line references and evidence against actual diff.
- Re-score severity when a subagent over- or under-states risk.
- Merge related findings sharing a root cause.
- Drop speculative / unsupported findings.
- Keep important-but-unproven concerns under **Needs verification**.
- Prefer actionable fixes over general advice.
- Surface cross-cutting themes only after concrete findings.
- If deep audit ran, preserve its coverage summary; drop its narrative.
- Classify failure behavior when relevant: success, handled failure, fail-open, fail-closed, unexpected exception, cancellation-safe, partial update.

### 5. Produce the final report

Use the **Final report format** below. Omit empty sections except Summary, Reviewed scope, and Final verdict. Include Coverage summary when deep audit ran or when coverage gaps materially affect confidence.

---

## General review rules (give to every subagent)

```
Each subagent must:
- Review only from its assigned perspective.
- Inspect surrounding context when the diff alone is insufficient.
- Prefer concrete findings over generic advice.
- Avoid style nitpicks unless they create real ambiguity, risk, or maintenance cost.
- Avoid duplicate findings already obvious from its own previous points.
- Mark uncertainty explicitly.
- Do not assume behavior not visible from code or provided context.
- If a finding depends on missing context, say exactly what context is missing.

False positives are costly. Do not report a finding unless there is a plausible
concrete risk, maintenance problem, user impact, or reviewability problem.
```

## Required subagent output format

```
Subagent: <name>
Scope: <one-line scope>

Findings:
1. Title: <short finding title>
   Risk score: <0-100>
   Confidence: high | medium | low
   Severity: blocker | major | minor | nit | follow-up
   Files/lines: <file:line or file/range; "unknown" only if unavailable>
   Evidence: <concrete evidence from diff/code>
   Risk: <why this matters>
   Proposed fix: <specific fix or next step>
   Notes: <optional caveats>

2. ...

Needs verification:
- <important but not fully proven concern, with exact missing context>
```

**Risk score guidance:**
- 90-100: likely blocker; correctness, data loss, security, serious compat, race/deadlock, severe hot-path regression.
- 70-89: major issue likely worth fixing before merge.
- 40-69: meaningful maintainability, operability, test, or perf concern.
- 20-39: minor but concrete improvement.
- 0-19: nit / optional cleanup; usually do not report.

---

## Subagents catalog

For each subagent: `Scope` is the one-line scope to put in the output header; `Prompt` is the body to pass.

### 1. UX / feature sanity / public contract

**Scope:** UX, feature coherence, public API and behavioral contract.

**Focus:** feature UX and logical consistency; whether behavior matches user expectations; public API and contract changes; exposed interfaces, defaults, error semantics, invariants; misuse-prone APIs; narrow implementations where a general idiomatic alternative exists; long-term maintainability of user-visible model; surprising behavior, misleading errors, confusing edges.

**Prompt:**
```
Review the diff from a UX, feature-sanity, and public-contract perspective.
Look for ad hoc behavior, confusing semantics, poor defaults, surprising error
behavior, unclear invariants, misuse-prone APIs, and places where the
implementation solves a narrow case while a more idiomatic general design
exists. Report only concrete issues with user or caller impact.
```

### 2. Code architecture / design

**Scope:** Architecture, design, component cooperation.

**Focus:** design inconsistencies; accidental complexity; unclear or implicit behavior; SOLID / responsibility violations; leaky abstractions; tight coupling / low cohesion; non-idiomatic or hard-to-use internal APIs; encapsulation / layering; partial / asymmetric fixes across similar components.

**Prompt:**
```
Review the diff for architecture and design issues: inconsistencies, accidental
complexity, unclear or implicit behavior, responsibility/SOLID violations,
leaky abstractions, tight coupling, low cohesion, non-idiomatic or hard-to-use
APIs, improper encapsulation/layering, and partial/asymmetric fixes across
sibling components or similar code paths.
Explain how the moving parts interact and where the design creates future
maintenance risk.
```

### 3. Ockham / YAGNI / unnecessary diff

**Scope:** Avoidable, unrelated, no-op, or premature changes.

**Focus:** unrelated cleanup mixed with core change; no-improvement edits; speculative generality; unnecessary abstractions; no-op rewrites; review-impeding churn; changes belonging in separate commits or follow-ups.

**Prompt:**
```
Review the diff for unnecessary diff: avoidable, unrelated, speculative, or
no-op changes that do not improve behavior, performance, clarity, safety, or
reviewability. Identify which changes should be removed, split into separate
commits, or postponed.
```

### 4. Security / SQL access control / trust boundary

**Scope:** Security-sensitive behavior and trust boundaries.

**Focus:** SQL access control / privilege checks; auth, ACLs, row policies, quotas, resource limits; server-side file access / path traversal; user-controlled paths, URLs, identifiers, settings, formats, table functions, config; leaked file content / secrets in errors; trust-boundary expansion (internal code newly exposed via SQL/API/user input); unsafe wrappers around previously internal functions; malformed / empty / minimal / huge / adversarial inputs; injection in SQL construction, shell calls, paths, logs, diagnostics.

**Prompt:**
```
Review the diff for security issues and trust-boundary expansion.
Pay special attention to SQL access control, user-controlled inputs,
server-side file access/path traversal, resource-limit bypasses, leaked
secrets in errors/logs, and internal code newly exposed to SQL/API/user input.
For suspicious callee code, compare old callers vs new callers and test
boundary assumptions with concrete minimal inputs.
```

### 5. Performance / hot-path

**Scope:** Runtime performance and scalability.

**Focus:** hot-path regressions; unneeded copies/moves; avoidable allocations, missing reserve, allocation in loops; virtual dispatch, missed inlining, std::function / type-erasure overhead in hot paths; cache-unfriendly access, pointer chasing, locality; wrong containers, high constant factors; repeated computation, missed caching/hoisting; pathological complexity, accidental O(N²); unbounded growth/allocations; extra disk/network roundtrips, syscalls, fsyncs, sleeps, polling; logging in hot paths.

**Prompt:**
```
Review the diff from a performance perspective, especially hot paths.
Look for unnecessary copies, allocations, dispatch/inlining overhead,
cache-unfriendly access, poor container choices, repeated work, pathological
complexity, unbounded growth, and inefficient disk/network/syscall behavior.
Report only issues that plausibly affect realistic workloads or scalability.
```

### 6. Documentation / comments / changelog

**Scope:** Understandability for users and maintainers.

**Focus:** missing user-facing docs; misleading / incomplete docs; comments for non-obvious logic; outdated comments; unclear error / log / diagnostic messages; changelog / release-note quality; migration notes for incompatible behavior; typos in user-visible strings, logs, docs, comments, identifiers.

**Prompt:**
```
Review documentation and explanatory quality: user-facing docs, comments for
non-obvious logic, changelog/release-note quality, migration notes,
diagnostics, logs, error messages, and typos. Do not ask for comments on
obvious code; focus on places where missing or misleading explanation creates
user or maintenance risk.
```

### 7. Code quality / correctness / maintainability

**Scope:** Local code quality and bug-proneness.

**Focus:** likely bugs / edge-case errors; clarity / readability; naming; defensive coding; fragile assumptions; error-prone control flow; duplicated logic; confusing conditionals; magic constants; unchecked return values; integer overflow / truncation / lossy conversions; invalid assumptions about nullability, emptiness, ordering, sizes.

**Prompt:**
```
Review the diff for code quality, correctness, and maintainability: likely
bugs, unclear code, poor naming, fragile assumptions, missing defensive
checks, duplicated logic, magic constants, unchecked failures, and error-prone
edge cases. Prefer concrete bug risks over style preferences.
```

### 8. Operability / DevOps / observability

**Scope:** Production introspection, debuggability, alerting.

**Focus:** introspection of behavior; debuggability of failure modes; useful logs / errors; metrics and perf counters; alertability of exceptional situations; missing context in diagnostics; safe degradation / recovery; background task visibility; operational impact of config / default changes.

**Prompt:**
```
Review the diff from an operability / DevOps perspective: introspection,
debugging, metrics, logging, alerting, production diagnostics, recovery, and
visibility into exceptional or background behavior.
Flag cases where production issues would be hard to detect, debug, or react to.
```

### 9. Header / include / compile-time impact

**Scope:** Header hygiene and build-time impact.

**Focus:** unnecessary includes; missing forward decls; excessive header deps; heavy transitive includes in high-fan-out headers; non-trivial function bodies in headers; template defs causing broad instantiation; large constexpr work in headers; binary size / compile-time regressions; deps that should move from header to .cpp.

**Prompt:**
```
Review the diff for header/include hygiene and compile-time impact:
unnecessary includes, missing forward declarations, excessive header
dependencies, heavy transitive includes, non-trivial code in headers,
unnecessary template instantiations, large constexpr work in headers, and
dependencies that should move to implementation files.
```

### 10. Concurrency / synchronization

**Scope:** Multithreading, shared state, synchronization.

**Focus:** shared mutable state; readers/writers and guards; data and lifetime races; missing / excessive / inconsistent locking; lock ordering, deadlocks; TOCTOU; atomics and memory ordering; condition variables; reentrancy; blocking I/O or callbacks under locks; async / task / thread-pool interactions; thread-safety of exposed APIs; undocumented thread-affinity or implicit ordering assumptions.

**Prompt:**
```
Review the diff for concurrency issues: shared mutable state, data races,
lifetime races, locking consistency, lock ordering/deadlocks, TOCTOU,
atomics/memory ordering, condition variables, reentrancy, blocking under
locks, and async/callback/thread-pool safety.
For each touched shared object, identify readers/writers, guards, relevant
call paths, and risky interleavings.
```

### 11. Tests / regression risk

**Scope:** Validation adequacy and regression coverage.

**Focus:** missing tests for changed behavior; weak assertions; deleted / relaxed / over-normalized tests; missing negative tests; missing boundary cases (empty, length 1, huge, malformed, null, invalid config); missing compatibility tests; missing security / concurrency / perf tests where relevant; brittle tests; gaps between implementation risk and coverage.

**Prompt:**
```
Review test coverage and regression risk: missing tests, weak assertions,
untested edge cases, deleted/relaxed tests, missing negative tests, missing
compatibility/security/concurrency/performance coverage, brittle tests, and
gaps between changed behavior and validation.
Suggest specific tests to add or strengthen.
```

### 12. Compatibility / rollout / migration

**Scope:** Cross-version, cross-deployment, cross-config safety.

**Focus:** backward / forward compatibility; config and settings changes; defaults and behavior changes; wire / storage / serialization / metadata / protocol formats; migrations and downgrade behavior; mixed-version clusters; feature flags / experimental gates; safe rollout in OSS and Cloud-like environments; compatibility settings preserving old behavior; existing data or objects that may violate new validation.

**Prompt:**
```
Review the diff for compatibility and rollout risks: backward/forward
compatibility, config/settings changes, defaults, wire/storage/serialization/
protocol formats, migrations, downgrade behavior, mixed-version clusters,
feature flags, experimental gates, and safe rollout.
Flag new validation or behavior changes that may break existing data,
queries, configs, or deployments.
```

### 13. C++ lifetime / ownership / exception safety

**Scope:** Object lifetime, ownership, exception safety.

**Focus:** dangling refs / pointers / views / spans / string_views; iterator/reference invalidation; move/copy semantics; moved-from object use; RAII correctness; cleanup on early returns / exceptions; exception safety / partial state updates; noexcept boundaries and new throw paths; destructor callbacks / scope guards / callback-triggered exceptions; shared_ptr / unique_ptr misuse; ownership ambiguity; cycles or double ownership.

**Prompt:**
```
Review the diff for C++ lifetime, ownership, and exception-safety issues:
dangling references/views, invalidation, move/copy mistakes, RAII
correctness, cleanup on early returns/exceptions, new throw paths crossing
noexcept/destructor boundaries, smart-pointer misuse, ownership ambiguity,
and partial-state updates.
Trace full caller chains when new exceptions or lifetime assumptions are
introduced.
```

### 14. Repository impact / generated / binary artifacts

**Scope:** Repository hygiene; accidental artifacts.

**Focus:** large files; binary files; accidentally committed generated files; vendored dependency blobs; archives / JARs / .so / executables / datasets / model artifacts; split / chunked binaries; build outputs; repo bloat; test data that should be generated or downloaded.

**Prompt:**
```
Review the diff for repository-impact issues: large files, binary artifacts,
generated files, vendored blobs, archives, compiled outputs, datasets, split
binaries, and unnecessary repository bloat.
Flag anything that should not be committed or should be generated/downloaded
at test time instead.
```

### 15. Deep audit / transition and fault-injection (high-risk only)

**Scope:** Deep transition and fault analysis for complex / high-risk changes.

**When to run:** state machines; config interactions; API/protocol flows; storage / metadata mutations; background tasks; replication / Keeper logic; security boundaries; concurrency-sensitive behavior. **Skip for trivial / local changes.**

**Focus:** call graph and entrypoints; caller assumptions and trust-boundary changes; request/event flow through validation, dispatch, state changes, outputs, side effects; key invariants before/after each transition; logical branch coverage; fault categories derived from actual changed components; malformed / empty / minimal / huge input, missing config, timeout, failed IO/network, exception, cancellation, shutdown, concurrent update; fail-open vs fail-closed for security paths; rollback / cleanup / partial-update safety; parser/config/runtime/API parity; cross-partition / cross-component interactions.

**Prompt:**
```
Review the diff in deep audit mode. First build a lightweight call graph and
transition matrix for the changed behavior: entrypoints, validation/dispatch,
state/storage/cache interactions, downstream integrations, state changes,
outputs, side effects, and error/exception propagation.

List the key invariants and check whether each transition preserves them.

Define logical fault categories from the actual code under review, then test
them by reasoning through success, handled failure, fail-open, fail-closed,
exception, cancellation, timeout, malformed input, boundary input, shutdown,
and concurrency/timing paths as applicable.

For mutation-heavy paths, analyze exception or cancellation after each
intermediate state change and verify rollback, cleanup, and invariants.

For critical shared-state paths, write plausible interleavings and check for
race, deadlock, lifetime, and partial-update hazards.

Report only confirmed defects. Keep speculative concerns under "Needs
verification". Include a short coverage summary: reviewed entrypoints,
transitions, fault categories, skipped areas, and assumptions.
```

---

## Final report format

```
# Review report: <diff spec / PR / branch>

## Summary
<neutral summary of the change and high-level verdict>

## Reviewed scope
- Diff: <input>
- Base: <base>
- Files changed: <count>
- Main moving parts:
  - <component>: <files / role>

## Missing context
- <omit section if none>

## Blockers
1. <finding>
   Risk score: <0-100>
   Sources: <subagents>
   Files/lines: <...>
   Evidence: <...>
   Impact: <...>
   Proposed fix: <...>

## Major issues
...

## Minor issues / improvements
...

## Needs verification
- <concern + exact missing check/context>

## Suggested commit / diff split
- Core refactoring / behavior changes:
  1. <step>
  2. <step>
- Separate follow-ups:
  - <unrelated cleanup / docs / tests / bug fix>

## Tests to add or strengthen
- <specific test suggestions>

## Coverage summary
- Entry points reviewed: <omit if not applicable>
- Transitions reviewed: <omit if not applicable>
- Fault categories checked: <omit if not applicable>
- Deferred / not covered: <omit if none>
- Main assumptions: <omit if none>

## Final verdict
Status: approve | request changes | block
Minimum required actions:
- <action>
```

Omit empty sections except Summary, Reviewed scope, and Final verdict.

---

## Tone and quality bar

- Strict but neutral.
- High-signal findings only.
- No generic checklists dumped on the user.
- No praise for every checked area; no "looks good" sections.
- Findings must be specific enough to act on.
- Suggest small surgical fixes over broad rewrites.
- Keep subagent output plain and structured so aggregation is reliable.

## Common mistakes

- Launching subagents serially instead of in one parallel batch.
- Letting subagents write the final user report (they must not).
- Forwarding low-confidence speculation as findings instead of "Needs verification".
- Skipping the neutral context step — reviewers then duplicate exploration work.
- Running deep audit (#15) on trivial diffs — wasteful.
- Including "looks good" sections or padding the report.
