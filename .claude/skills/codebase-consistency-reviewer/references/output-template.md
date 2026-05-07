# Output Template

Produce the final review using this structure. Sections may be omitted when empty. For a small review, the Search Appendix may collapse to a single paragraph.

---

# Codebase Consistency Review

## Scope

- **Input**: PR / commit range / diff source / patch source.
- **Changed files**: list.
- **Repository search available**: yes / no.
- **Subagents**: parallel / sequential / none.
- **Detected language(s) / framework(s)**: list.
- **Limitations**: anything that constrained the review (e.g., no repo access, partial diff, paywalled docs, no PR description).

## Executive Summary

A short paragraph naming the most important findings, followed by counts:

- Exact reuse opportunities: N
- Likely duplication: N
- Abstraction / generalization opportunities: N
- Naming / API consistency issues: N
- Settings / config consistency issues: N
- Schema / data-model consistency issues: N
- Standard-mechanism consistency issues: N
- Checked but not flagged: N

## Findings

For each actionable finding:

### Finding N: <short title>

- **Severity**: Blocker / Strong recommendation / Suggestion / Observation
- **Category** (one of):
  - Existing function/class/helper should be reused
  - Existing abstraction should be extended
  - Duplicate or near-duplicate implementation
  - Inconsistent naming
  - Inconsistent user-facing setting/API
  - Inconsistent schema/data model
  - Non-standard codebase mechanism
  - Consider generalizing existing code
  - Borrow existing design ideas
  - Related precedent exists, but no direct reuse recommended
- **Changed code**: file path, symbol/setting/field/API, line references.
- **Existing precedent**: file path, symbol/setting/field/API/test/doc, line references.
- **Analysis**: what is similar, what is different, whether the precedent is direct reuse / a stronger abstraction / naming-API precedent / standard mechanism precedent / only related design; whether ownership or layering affects the recommendation. Be specific. Do not just say "similar code exists."
- **Recommendation**: a concrete action.
- **Suggested patch direction** (optional): short pseudo-diff or replacement sketch when the surrounding context is sufficient. Do not invent exact code otherwise.
- **Confidence**: High / Medium / Low.

## Checked But Not Flagged

Concise list of significant targets investigated without raising a finding. For each:

- Target.
- What was checked.
- Why no finding was raised (e.g., precedent only superficially similar; PR description justifies divergence; existing code is legacy/unsuitable for reuse).

## Search Appendix

Compact appendix (single paragraph for small reviews):

- Key targets investigated.
- Subagents used; parallel or sequential.
- Most relevant search anchors and query expansions (synonyms, lifecycle pairs, domain terms scraped from nearby files).
- Important files / tests / docs inspected.
- Important uncertainty (areas not fully searched, queries that returned noise, candidates not opened).
- Search limitations (no repo, no docs, no PR description, language unfamiliar to local tooling).

---

## Severity Labels

- **Blocker** — Strong duplication of working code, or a user-facing inconsistency expensive to undo once shipped. Reuse / alignment is clearly correct and feasible.
- **Strong recommendation** — Clear precedent exists; not aligning will create real maintenance cost or visible divergence.
- **Suggestion** — Precedent exists; alignment would be cleaner but divergence is defensible.
- **Observation** — Related precedent worth knowing about; no action required.

## Category Labels

Use the canonical names above so future readers can scan by category.

## Examples of Good Recommendations

- "Use `RetryPolicy::withExponentialBackoff` from `core/retry.rs:42` instead of the custom loop in `worker.rs:118-156`. Same semantics, already covered by tests in `core/retry_tests.rs`."
- "Move the path-normalization logic in `ingest/upload.go:88-104` into the existing `pathutil.Normalize` (`pathutil/normalize.go:12`) and call it from both ingest and export. Current divergence already produces different behavior on trailing slashes."
- "Rename setting `query_evaluation_timeout_seconds` to `max_query_evaluation_seconds` for consistency with `max_*_seconds` settings already in `Settings.h` (`max_execution_time`, `max_session_timeout_seconds`)."
- "Register the new background job through `BackgroundJobRegistry` (`bg/registry.cpp:55`) instead of starting a thread directly in `Refresher::start`. Every other background job in this codebase is registered through that mechanism."
- "This code cannot directly reuse `LRUCache` because it needs TTL eviction, but it should borrow the slot-eviction pattern from `LRUCache::evict` (`cache/lru.cpp:201`) rather than the linear scan in the new code."
- "The new column `last_refresh_ts` should be named `last_refresh_time` to match `last_modification_time` and `last_load_time` in sibling system tables."
- "No direct reuse is recommended; align the error message format with `formatRetryError` (`errors/format.cpp:30`) so operator-facing logs stay consistent."

## Examples of Weak Findings That Should Stay Out

These belong in *Checked But Not Flagged* (or be omitted entirely):

- "There's another function called `validate` elsewhere in the codebase." — name match without semantic match.
- "This module also uses retries somewhere." — no concrete reuse, generalization, or naming alignment proposed.
- "Could possibly be combined with X" with no reason why combining would be correct or beneficial.
- "Style differs from another file" — unless tied to a documented convention or visible user-facing surface.
- "This pattern appears in five places" — without identifying the canonical one or proposing alignment.

## Collapsing Sections for Small Reviews

For a small change with one or two targets:

- Keep **Scope**, **Findings**, and a one-paragraph **Search Appendix**.
- Drop **Executive Summary** counts; replace with a single sentence.
- Drop **Checked But Not Flagged** if empty.

For a no-finding review:

- Keep **Scope**, a one-paragraph **Executive Summary** stating no actionable findings, **Checked But Not Flagged**, and a one-paragraph **Search Appendix**.
