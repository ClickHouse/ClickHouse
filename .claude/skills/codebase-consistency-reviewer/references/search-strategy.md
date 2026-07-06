# Search Strategy

Subagents have **lexical** tools (`rg`/`grep`, file reads, repo navigation) and reasoning. There is **no vector or embedding search**. "Semantic search" in this skill means: drive lexical search with semantically generated queries, then **read candidates and judge semantic match by reasoning over the code**.

## Building a Search Brief

For each target, the brief should contain:

- **Target summary** — one or two sentences describing the concept.
- **Changed files and symbols** (when available).
- **Concept / responsibility** introduced or changed.
- **Why this could overlap** with existing code/conventions.
- **Observable behavior** — inputs, outputs, side effects, lifecycle.
- **Contract surface involved** — user-facing names, settings, fields, flags, metrics, errors, API shapes.
- **Literal identifiers from the new code.**
- **Possible synonyms / alternative names** the codebase may use.
- **Nearby anchors** likely to reveal true precedent (sibling files, related modules, related tests/docs).
- **Required anchors vs. optional hints** — which must hold for a match to be valid, which are just clues.
- **Ownership / module / layering boundaries** relevant to reuse.
- **What would count as** an exact duplicate / near duplicate / reusable abstraction / standard mechanism / naming-API precedent / related design / weak match.

Convert each target into one or more **consistency questions**:

- Is there already a canonical way to implement this responsibility?
- Is there a helper, abstraction, class, policy object, lifecycle hook, or framework mechanism for this?
- Does this subsystem already expose the same concept under another name?
- Are equivalent fields/columns/settings/parameters named differently elsewhere?
- Is this error/log/metric/config/test shape consistent with similar behavior?
- Do neighboring modules solve this with a registry/factory/visitor/enum/trait/interface/hook?
- Is this code introducing a custom mechanism for something the codebase usually centralizes?
- Should this be direct reuse, generalization, naming alignment, or design alignment?

## Generating a Wide Query Set

The target is the **concept**, not the names. New code may use the wrong name. Expand semantically before searching.

Start from literal identifiers, then add:

- **Synonyms / paraphrases**:
  - retry → backoff, reattempt, redo, again
  - lock → mutex, guard, exclusive, semaphore, latch
  - cache → memoize, store, buffer, pool
  - validate → check, verify, ensure, assert
  - schedule → queue, dispatch, defer, plan
- **Action-noun / noun-verb forms**: schedule / scheduler / scheduling; convert / conversion / converter; refresh / refresher / refreshing.
- **Lifecycle and opposite pairs**: acquire/release, open/close, start/stop, register/unregister, subscribe/unsubscribe, init/shutdown, begin/end.
- **Broader and narrower terms**: "exponential backoff" → also try "backoff" and "retry"; "least-recently-used" → also try "eviction".
- **Domain vocabulary the codebase actually uses** — scan nearby files (sibling source, tests, docs) and add the words they use. This is the highest-yield expansion source.
- **Imports and library calls** the new code uses — they often anchor existing precedent.
- **Test names and doc strings** near the change.

Generate the query set first, then search. Don't search one term, look at one result, and stop.

## Driving Lexical Search

- Use `rg` (or `grep -R`) with the query set.
- Search for symbols, type names, config keys, error text, log keys, metric names, test names.
- Search documentation, schema files, CLI definitions, settings tables.
- Use boundary-aware patterns when names are short or generic.
- Cast wide first; then narrow once a promising area is found.

## Reading Candidates (Don't Match Names)

A lexical hit is a **lead, not a verdict**. Open the candidate and judge semantic match by comparing:

- purpose / responsibility
- shape (signature, fields, schema, flag spelling)
- behavior (inputs, outputs, side effects)
- lifecycle (when created, when destroyed, when called)
- error model and reporting
- concurrency / resource constraints
- ownership and layering

A function called `validate_user` may or may not be the same concept as a new `check_user_input` — only reading both can tell.

## Search in Layers, Local-First

1. Same file and directly adjacent files.
2. Same module / package / subsystem.
3. Tests for the same subsystem.
4. Shared utilities, framework code, common abstractions.
5. Similar concepts elsewhere in the repository.

Prefer local precedent over distant precedent unless the distant precedent is clearly a global standard (e.g., a project-wide error class, a project-wide config schema, a project-wide registry).

## Tests and Docs as Precedent Sources

- **Tests** reveal canonical constructors, fixtures, expected errors, expected config shapes, and behavior boundaries. They often pin down conventions code alone hides.
- **Docs** reveal user-facing terminology that should govern naming even when internal code uses different terms.

## Required vs. Optional Anchors

Some matches are only valid with specific anchors. Require them when relevant:

- A **settings match** may require the same subsystem, config parser, or user-facing option layer.
- A **table/column naming** comparison may require the same table family or adjacent schema.
- An **algorithm** comparison may require the same input/output semantics.
- An **API consistency** comparison may require the same public API layer.
- A **direct-reuse** recommendation requires compatible ownership and dependency direction.

If the required anchor doesn't hold, demote the finding (precedent only, not reuse) or drop it.

## Avoid Overfitting to New Code's Names

If you only search the new identifiers, you'll miss the precedent that uses the codebase's actual vocabulary. Always include synonyms and domain terms scraped from nearby files.

## Detecting Bypassed Standard Mechanisms

Look explicitly for hand-rolled versions of mechanisms the codebase usually centralizes:

- registries, factories, visitors
- configuration schemas, feature flags
- error classes, logging helpers, metrics helpers
- serialization frameworks, lifecycle hooks
- dependency injection, test utilities
- naming conventions, schema conventions

If the codebase routinely registers things through mechanism X and the new code adds a custom path that does the same job, that's a finding even if the implementation is correct.
