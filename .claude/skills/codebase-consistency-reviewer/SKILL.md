---
name: codebase-consistency-reviewer
description: Use when asked to review a PR, commit, commit range, branch, patch, or pasted diff for duplicated functionality, reinvented wheels, not-invented-here patterns, parallel abstractions, inconsistent naming, inconsistent settings/APIs/schemas/metrics/errors/logs, redundant config keys, non-standard implementations, or places where new code should reuse, generalize, extend, or align with existing codebase patterns and user-facing conventions. Triggers on phrases like "review for duplication", "consistency review", "is this already implemented", "does this match our conventions", "reinventing the wheel".
---

# Codebase Consistency Reviewer

## Purpose

This is **not** a correctness or bug review. The lens is:

- Is this functionality already implemented somewhere else, possibly under another name?
- Is there an established helper, abstraction, API, setting, schema, or naming pattern to reuse?
- Does the new code follow the standard mechanism this codebase uses for this kind of job?
- Do new user-facing names/settings/fields/metrics/errors/logs match existing conventions?
- Should the change generalize an existing implementation instead of paralleling it?
- If direct reuse isn't appropriate, should it borrow concepts from related code?

Report **actionable consistency risks**. Skip weak similarities.

## Inputs

Accept any of: PR number, PR URL, commit SHA, commit range, branch name, local patch/diff text, pasted changed files, repo path or URL. Ask for missing input only when the review cannot proceed.

## Workflow

### 1. Intake & capability detection

Detect what's available before planning:
- Local repo? (`git`, file reads, `rg`/`grep`)
- GitHub access? (`gh` CLI, GitHub MCP, web fetch)
- Subagents available for parallel lookup?
- Single language/framework or many?

If repo access exists, retrieve: the diff, changed files with surrounding context, relevant imports, touched public/semi-public APIs, touched settings/config schemas, touched CLI flags / env vars / table schemas / protocol fields / user-facing names, related tests, related docs, **and the PR description, linked issues, and commit messages** (the author may have justified deliberate divergence — weight stated intent before flagging).

If repo access is unavailable, work from the supplied material and state the limitation in Scope.

The main reviewer infers language/framework/conventions from the changed files. Do not assume — let the codebase tell you what "standard mechanism" means here.

### 2. Identify review targets

Extract **concept-bearing and convention-bearing** changes — not every changed line.

**Cluster** related changes into one target when they cooperate to do one job (a lock spread across methods + a thread + a state field is one target, not four).

**Skip trivial changes**: getters/setters, one-line delegations, plumbing, parameter forwarding, mechanical refactors, formatting, trivial renames, bug fixes that introduce no new concept, local helpers too small to plausibly have a canonical version.

**Prioritize** targets that:
- create or change a contract surface (API, setting, flag, schema field, table column, metric, log field, error, persisted/wire format, documented behavior, user-visible name)
- implement a recognizable responsibility (lifecycle, retry, timeout, caching, scheduling, locking, validation, parsing, formatting, lookup, conversion, matching, resource management)
- introduce an abstraction, helper, registry entry, extension point, or composition pattern others may copy
- look like they bypass or recreate a standard codebase mechanism
- overlap semantically with concepts that may already exist under another name
- establish a name/API/setting/test/operational convention that should match precedent

Pay special attention to "small" changes that encode durable conventions: a new column, config key, enum value, metric name, log attribute, error code, or any name users/operators/developers/downstream systems will see.

For each selected target, separate:
- the **semantic concept** introduced
- the **literal identifiers** in the new code
- **likely existing names/synonyms** elsewhere
- the **contract surface** involved, if any
- **ownership/layering** boundaries that may affect reuse
- **what kind of precedent** would help: direct reuse / reusable abstraction / naming-API precedent / setting-config precedent / schema precedent / standard-mechanism precedent / related design precedent

Filter aggressively on triviality, then investigate every remaining target. Do **not** artificially cap target count.

See `references/review-rubric.md` for target-selection principles, clustering rule, triviality filter, severity definitions, finding categories, and false-positive guidance.

### 3. Build search briefs

For each target, create a concise brief. The **target is the concept**, not the names; new code may use the wrong name. Search both the new identifiers and the underlying concept.

Brief contents and concrete query-generation tactics (synonyms, lifecycle pairs, broader/narrower terms, domain vocabulary scraped from nearby files): see `references/search-strategy.md`.

### 4. Delegate lookup investigations to subagents

For each non-trivial target, dispatch a **separate** subagent. Run them **in parallel** when subagent tooling supports it; otherwise sequentially (note this in the Search Appendix).

Each subagent receives only: the target's search brief, the relevant diff excerpt, and enough surrounding context. They search for evidence and **return references and evidence, not final recommendations**.

Subagent instructions and the compact report format: see `references/lookup-worker-instructions.md`.

### 5. Search strategy (no vector search assumed)

"Semantic search" here means: drive **lexical** tools (`rg`/`grep`) with semantically-generated queries, then **read candidates and judge semantic match by reasoning over the code**. Names are weak signals — open the file before believing the match.

Search local-first (same file → adjacent files → module → tests → shared utilities → global). Use tests and docs as precedent sources (they reveal canonical constructors, fixtures, expected errors, user-facing terminology). Look explicitly for **bypassed standard mechanisms** (registries, factories, config schemas, error classes, logging/metrics helpers, lifecycle hooks, DI, test utilities, naming/schema conventions).

Full strategy: `references/search-strategy.md`.

### 6. Validate findings (do not trust subagents blindly)

For each candidate finding, the main reviewer compares new code and precedent on: semantic purpose, behavior, inputs/outputs, side effects, lifecycle, error semantics, performance/concurrency constraints, ownership, layering, dependency direction, public-API implications, compatibility, test coverage, and whether the precedent is current vs. legacy/deprecated/special-purpose. Check whether the PR description already justifies divergence.

Promote a target to a finding only when there's a **concrete, actionable recommendation or a meaningful consistency risk**. Park inconclusive or non-actionable matches in "Checked but not flagged".

Do not:
- recommend reuse that creates bad coupling or violates ownership/layering
- flag superficial name similarity as duplication
- ignore naming/setting/schema/API consistency just because implementations differ
- recommend generalization for its own sake
- penalize deliberate divergence the PR description already explains

### 7. Produce the report

Use the structure in `references/output-template.md`:

- **Scope** — inputs, changed files, capabilities used, parallel vs sequential, language/framework, limitations
- **Executive Summary** — most important findings with counts by category
- **Findings** — each with Severity, Category, Changed code refs, Existing precedent refs, Analysis, Recommendation, optional patch sketch, Confidence
- **Checked But Not Flagged** — significant targets investigated without raising a finding
- **Search Appendix** — targets, subagents (parallel or sequential), key anchors/query expansions, files/tests/docs inspected, uncertainty, limitations

Sections may be omitted when empty. Small reviews may collapse the Search Appendix to one paragraph.

## Style

- Practical, specific, codebase-aware. Concrete recommendations over abstract advice.
- Don't overclaim. "Conceptually similar" stays "conceptually similar."
- Local conventions govern local code; global conventions govern shared/public surfaces.
- User-facing knobs (settings, names, APIs, schema fields, metrics, logs, errors, persisted formats) get extra attention — those inconsistencies are expensive to fix later.
- Keep the final report focused on **actionable** risks. Don't dump every weak similarity from search.

## References

- `references/review-rubric.md` — target selection, clustering, triviality, severity, categories, FP guidance
- `references/search-strategy.md` — briefs, query generation, lexical-driven semantic search, layering, anchors
- `references/lookup-worker-instructions.md` — subagent dispatch and report format
- `references/output-template.md` — final report structure and finding format
