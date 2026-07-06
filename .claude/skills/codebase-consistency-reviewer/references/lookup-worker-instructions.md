# Lookup Worker (Subagent) Instructions

The main reviewer dispatches one **lookup subagent per important review target** (or per cluster). Subagents are evidence gatherers, not deciders.

## Dispatch Rules

- **One subagent per target.** Keep investigations independent — do not give a subagent multiple unrelated targets.
- **Run in parallel** when the available subagent tooling supports concurrent agents. If parallelism is unavailable, run sequentially and note this in the Search Appendix.
- Use **cheaper / faster** subagents (e.g., a general-purpose or Explore-style search agent) for breadth.
- Reserve the main reviewer for target selection, clustering, evidence validation, and final recommendations.

## What Each Subagent Receives

Only what is needed for the investigation:

- the **target search brief**
- the relevant **diff excerpt** for the target
- enough **surrounding context** (imports, related symbols, neighboring files) to understand the target
- explicit instructions to **search for existing precedent**
- explicit instructions to **return references and evidence, not final recommendations**
- a pointer to `references/search-strategy.md` for how to search

Do **not** pass other targets, the full PR, the full report scaffolding, or unrelated guidance.

## What Subagents Should Search For

- Exact or near-duplicate implementations of the target's responsibility.
- Existing helpers, abstractions, classes, registries, factories, or framework mechanisms that already do this job.
- Similar user-facing APIs, settings, flags, fields, columns, metrics, log keys, errors, or schema elements.
- Naming precedents (sibling settings, sibling columns, sibling metrics, sibling errors).
- Tests and docs that pin down canonical shapes or terminology.
- Patterns used by neighboring subsystems for the same kind of job.
- Semantically similar implementations under different names (use synonym/lifecycle/broader-narrower expansion).
- Standard mechanisms the new code may be **bypassing** or **recreating**.

The strategy (lexical-driven semantic search, layered local-first searching, query expansion, anchor handling, candidate reading) is described in `search-strategy.md`. Follow it.

## What Subagents Should NOT Do

- Do **not** make final review decisions.
- Do **not** decide severity.
- Do **not** classify a finding as a Blocker / Strong recommendation / Suggestion / Observation.
- Do **not** recommend reuse vs. generalization vs. naming alignment as a final answer — present these as possibilities for the main reviewer.
- Do **not** invent code; if patch direction is unclear, say so.

## Subagent Report Format

Return a compact report with:

- **Target summary** — one or two sentences in your own words.
- **Files, symbols, or contract surfaces changed** — what you understood the new code to be.
- **Search strategy used** — query set tried, layers covered, files inspected.
- **Important search anchors** — which queries were high-yield, which were noise.
- **Possible existing implementations or related patterns** — each with:
  - file path, symbol/setting/field/API, line numbers when possible
  - one-sentence description of what it does
  - why it may be relevant
  - what would have to be true for it to be a real match (anchors)
- **Similarity classification** for each candidate, picking from:
  - exact duplicate
  - near duplicate
  - same abstraction exists
  - same standard mechanism exists
  - same concept implemented differently
  - naming/API inconsistency
  - setting/config inconsistency
  - schema/data-model inconsistency
  - related design precedent only
  - no meaningful precedent found
- **Confidence** — high / medium / low.
- **Why the match may matter** — coupling cost, divergence cost, user-facing inconsistency cost.
- **Reuse / generalization / naming-only signals** — for the main reviewer's judgment, not as a recommendation:
  - whether direct reuse may be possible
  - whether generalization may be appropriate
  - whether only naming/API/setting alignment may be relevant
- **Ownership / layering concerns.**
- **Uncertainties** — reasons the evidence may be weak, areas not searched, queries not tried.

Keep the report compact. Reference file:line. Do not paste large code blocks unless essential.

## Confidence and Uncertainty Guidance

- **High confidence** — read both the new code and the candidate, semantic match is clear, ownership/layering compatible, anchors hold.
- **Medium confidence** — read both, semantic match is plausible but partial, or anchors are partial.
- **Low confidence** — name/shape resemblance only, candidate not fully read, anchors weak.

State uncertainties explicitly. The main reviewer needs to know what was not searched as much as what was.

## Independence

Each subagent works without seeing other subagents' work. The main reviewer integrates and deduplicates. If two subagents independently surface the same precedent, that's a stronger signal — the main reviewer treats it as such during validation.
