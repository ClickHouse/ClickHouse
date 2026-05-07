# Review Rubric

## Target Selection Principles

A **review target** is any non-trivial part (or cluster) of the change that may plausibly have:

- an existing implementation elsewhere
- an established abstraction or helper to reuse
- a standard mechanism the codebase uses for this kind of job
- a conventional way of being named, configured, exposed, tested, logged, or documented
- a precedent in neighboring subsystems
- a risk of creating a parallel API, setting, schema field, lifecycle, error model, algorithm, or operational pattern
- a future maintenance cost if implemented inconsistently

### Prioritize targets that

- Create or change a **contract surface**: API, public/semi-public function, setting, flag, schema field, table column, metric, log field, error type, persisted format, wire format, documented behavior, user-visible name.
- Implement a **recognizable responsibility**: lifecycle, retry, timeout, caching, scheduling, locking/synchronization, validation, parsing, formatting, lookup, selection, matching, conversion, resource management.
- Introduce an **abstraction, helper, wrapper, adapter, registry entry, extension point, integration point, or composition pattern** future code may copy.
- Appear to **bypass or recreate a standard codebase mechanism**.
- **Overlap semantically** with a concept that may already exist under another name.
- Establish a **name, API shape, setting shape, test pattern, or operational convention** that should match nearby or global precedent.
- Make a **small but durable user/operator/developer/downstream-system-facing decision**.

### Pay special attention to "small" changes that encode conventions

- Adding a column, field, config key, enum value, metric, log attribute, or error code.
- Introducing a name users, operators, developers, or downstream systems will see.
- Adding a helper whose behavior sounds generic.
- Adding local logic that looks like parsing, normalization, validation, formatting, lookup, conversion, matching, scheduling, or selection.
- Adding a second path to do something that already has a canonical path.
- Exposing existing behavior through a new layer, interface, or name.

## Clustering Rule

Cluster cooperating changes into a single target when reviewing them separately would fragment the analysis. Cluster when:

- the pieces only make sense together
- they share one user-visible / operator-visible concept
- they implement one responsibility distributed across files

**Examples of clustered targets:**
- A lock implemented across several methods + a background thread + a state field → one target.
- A new retry policy spread across a wrapper + a config key + an error type → one target.
- A new metric defined in one place, emitted in another, documented in a third → one target.

## Triviality Filter (skip these)

- Getters, setters, simple accessors.
- One-line delegations / pass-throughs.
- Pure plumbing, parameter forwarding, formatting, trivial renames.
- Mechanical refactors with no new contract surface.
- Bug fixes inside existing logic that introduce no new concept.
- Local helpers too small to plausibly have a canonical version elsewhere.

If the change is too trivial to plausibly have a "right way" elsewhere, skip it.

## Examples

**Concept-bearing changes (targets):**
- New retry/backoff policy.
- New cache or memoization layer.
- New scheduler, queue, or executor.
- New configuration key or CLI flag.
- New error class or error code.
- New metric, log field, or trace attribute.
- New schema field or table column.
- New parsing/validation/normalization helper.
- New lifecycle hook, registry, or extension point.
- New user-visible name (function in public API, setting, command, doc term).

**Convention-bearing changes (targets):**
- A new option spelling that other options in the same subsystem already follow.
- A new column whose name diverges from sibling columns.
- A new error message style that diverges from existing style.
- A new test fixture pattern.

**Non-targets:**
- Renaming a private variable.
- Splitting a function for readability with no new contract.
- Adding logging at a single existing call site, using existing logger.
- Tightening a type without behavior change.
- Inlining a constant.

## Severity Definitions

- **Blocker** — Strong duplication of working code, or a user-facing inconsistency (setting/API/schema/metric/error/log) that will be expensive to undo once shipped. Reuse or alignment is clearly correct and feasible.
- **Strong recommendation** — Clear precedent exists; not aligning will create real maintenance cost or visible divergence. Reasonable to push back on.
- **Suggestion** — Precedent exists and alignment would be cleaner, but divergence is defensible.
- **Observation** — Related precedent worth knowing about; no action required.

## Finding Categories

- Existing function/class/helper should be reused
- Existing abstraction should be extended
- Duplicate or near-duplicate implementation
- Inconsistent naming
- Inconsistent user-facing setting/API
- Inconsistent schema/data model
- Non-standard codebase mechanism (bypassed standard mechanism)
- Consider generalizing existing code
- Borrow existing design ideas (no direct reuse)
- Related precedent exists, but no direct reuse recommended

## Distinguishing Match Types

Don't collapse all similarity into "duplication". Differentiate:

- **Direct reuse opportunity** — call existing code instead.
- **Abstraction opportunity** — generalize existing code; both new and old call it.
- **Naming/API precedent** — implementations may differ; align surface.
- **Setting/config precedent** — align option name/shape/parser.
- **Schema/data-model precedent** — align field/column naming and shape.
- **Standard-mechanism precedent** — register through the existing mechanism (registry, factory, hook, config schema, error/logging/metrics helper, etc.).
- **Related design precedent** — borrow the *idea*; direct reuse not appropriate.
- **Weak similarity** — surface resemblance only; do not flag.

## Avoiding False Positives

- Don't flag superficial name similarity. Open both files and compare semantics.
- Don't recommend reuse across ownership boundaries unless the boundary already permits it.
- Don't recommend reuse of legacy/deprecated/special-purpose code.
- Don't ignore the PR description: stated intent for divergence weighs against flagging.
- Don't recommend generalization for its own sake — only when it prevents divergent behavior or reduces real maintenance cost.
- A name match without a behavior match is *naming* precedent, not duplication. Classify accordingly.
- A behavior match with different ownership/layer may still warrant a *standard-mechanism* finding even if direct reuse is wrong.
