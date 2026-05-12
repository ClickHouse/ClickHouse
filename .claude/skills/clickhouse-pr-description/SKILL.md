---
name: clickhouse-pr-description
description: Generate PR descriptions for ClickHouse/ClickHouse that match maintainer expectations. Use when creating or updating PR descriptions.
argument-hint: "[PR-number or branch-name]"
disable-model-invocation: false
allowed-tools: Task, Bash(gh:*), Bash(git:*), Read, Glob, Grep, AskUserQuestion
---

# ClickHouse PR Description Skill

Generate a good PR description and apply it, with optional confirmation based on user preference.

## Title

Plain description of what changed. No prefix conventions like `fix():` or `feat():`.

- Good: `Change default stderr_reaction to log_last for executable UDFs`
- Good: `Fix exception when inserting NULL into non-nullable column via CAST`
- Bad: `fix(udf): Change default stderr_reaction` — conventional commits, ClickHouse doesn't use them
- Bad: `Fuzzer fixes` — too vague, tells the reviewer nothing
- Bad: `Improve error handling and enrich exit code exceptions with stderr context` — too vague, doesn't say what was actually changed

## Body

Read `.github/PULL_REQUEST_TEMPLATE.md` for the exact template structure.

A good PR description has two parts:

**1. Free-form context (above the template, optional but encouraged for non-trivial changes)**

Explain to a future reader: what was the problem, why did it need fixing, what approach was taken, what were the results or trade-offs. This is the right place for:
- Motivation and background
- Description of the approach and why it was chosen over alternatives
- Benchmark results or measurements
- Links to related issues, discussions, or previous attempts
- Any caveats or known limitations

Use plain paragraphs. Headers like `## Motivation`, `## Changes`, `## Results` are fine and helpful — they make the PR easier to navigate for reviewers. Bullet lists are fine. Be as detailed as needed.

**2. Template section (mandatory)**

Fill in the changelog category (delete the rest of the list) and write the changelog entry when the selected category requires one.

### Example of a well-written body

From [#96110](https://github.com/ClickHouse/ClickHouse/pull/96110):

```
### Changelog category (leave one):
- Backward Incompatible Change

### Changelog entry:
The semantics of the `do_not_merge_across_partitions_select_final` setting were
made more obvious. Previously, the feature could be automatically enabled when
the setting was not explicitly set in the configs. It caused confusion repeatedly
and, unfortunately, led to some issues in production. Now, the rules are simpler:
`do_not_merge_across_partitions_select_final=1` enables the functionality
unconditionally. If `do_not_merge_across_partitions_select_final=0`, then automatic
is used only if the new setting
`enable_automatic_decision_for_merging_across_partitions_for_final=1` and not used
otherwise. To preserve the old behaviour as much as possible, the defaults were set
to `do_not_merge_across_partitions_select_final=0` and
`enable_automatic_decision_for_merging_across_partitions_for_final=1`.

---

The backward-incompatible part is that if someone has
`do_not_merge_across_partitions_select_final=0` explicitly set in the configs,
it no longer protects against the use of automatics. I'm open to discussion on
whether we should conservatively default to disabled automatics.
```

Note: extra context after `---` is fine — reviewers see it, but only the changelog entry above the blank line goes into the CHANGELOG.

## Changelog entry guidelines

The entry is what ends up in the published CHANGELOG. Write it for a user who is upgrading and scanning for what affects them. The PR link and author attribution are appended automatically by tooling — do not include them.

**Format:** the changelog script (`tests/ci/changelog.py`) collects all lines after the `### Changelog entry:` header up to the first blank line, then joins them with spaces into a single string. This means:
- The entry can span multiple lines in the template — they become one paragraph
- A blank line terminates collection — anything after it is ignored
- Write it as a single paragraph (no bullet lists, no blank lines within)

**Tense:** mixed is fine and natural. "Added X", "Fix a case where...", "The `X` setting is now Y" are all real examples from the CHANGELOG. Don't force everything into present tense.

**Length:** match the impact. A small new function can be one sentence. A changed default or backward-incompatible behavior warrants as many sentences as needed, including what users must do to adapt.

**Specificity:** always name the exact thing that changed. Never write "Fix a bug" or "Improve performance" without saying what specifically.

**For backward-incompatible changes:** always explain the old behavior, the new behavior, and how to restore the old one if possible.

**Real examples from the ClickHouse CHANGELOG:**

One sentence, sufficient for a focused addition:
> Add `xxh3_128` hashing function.

One sentence with enough context:
> `DATE` columns from PostgreSQL are now inferred as `Date32` in ClickHouse (in previous versions they were inferred as `Date`, which led to overflow of values outside a narrow range). Allow inserting `Date32` values back to PostgreSQL.

Full migration instructions for a default change:
> Deduplication is turned ON for all inserts by default. It was OFF before for async inserts and for MV's, but it was ON for sync inserts. The goal is to have the same defaults for both ways of inserts. If you have deduplication explicitly disabled on your cluster, you have to explicitly set `deduplicate_insert='backward_compatible_choice'` to keep the old behavior.

Describes new capability with enough detail to understand when to use it:
> Added `OPTIMIZE <table> DRY RUN PARTS <part names>` query to simulate merges without committing the result part. It may be useful for testing purposes: verifying merge correctness in the new version, deterministically reproducing merge-related bugs, and reliably benchmarking merge performance.

Reference the issue if one exists: `Closes #XXXXX` or `Fixes #XXXXX` at the end.

## What to avoid

These patterns make PRs harder to read or signal low effort:

- `fix(scope):` / `feat():` / `chore():` — ClickHouse doesn't use conventional commits
- `This PR ...` to start any section — just describe the change
- Vague titles like `Fuzzer fixes`, `Fix bug`, `Improvements` — always say what specifically
- Markdown tables comparing "before/after behavior" unless genuinely useful
- Perfectly parallel sentence structure throughout — vary phrasing naturally

## AI co-authorship

It's fine to mention AI assistance openly. `Co-Authored-By:` in commits or acknowledgements in the PR description are acceptable — ClickHouse is open about AI-assisted development.

Before creating or updating the PR, check user memory for a confirmation preference. If no preference is stored and the session is interactive, ask once: "Should I always show you the description for approval before applying it, or just go ahead every time?" Save the answer to memory and apply it from then on. If the session is non-interactive, proceed directly without asking.

## Fork vs upstream

If the current repository is a fork (i.e. `git remote get-url origin` does not contain `ClickHouse/ClickHouse`), always target the upstream repository. Pass `--repo ClickHouse/ClickHouse` to `gh pr create` and set `--head <fork-owner>:<branch>` so the PR is opened against the canonical repo, not the fork.
