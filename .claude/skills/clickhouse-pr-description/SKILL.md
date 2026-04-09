---
name: clickhouse-pr-description
description: Generate PR descriptions for ClickHouse/ClickHouse that match maintainer expectations. Strips AI slop automatically. Use when creating or updating PR descriptions.
argument-hint: "[PR-number or branch-name]"
disable-model-invocation: false
allowed-tools: Task, Bash(gh:*), Bash(git:*), Read, Glob, Grep
---

# ClickHouse PR Description Skill

Generate PR descriptions that look like a human wrote them in 30 seconds. ClickHouse maintainers hate AI slop.

## Arguments

- `$0` (optional): PR number or branch name. If omitted, uses the current branch.

## Obtaining the Diff

**If a PR number is given:**
- Fetch the PR info: `gh pr view $0 --json title,body,baseRefName,headRefName,files`
- Get the diff: `gh pr diff $0`

**If a branch name is given:**
- Get the diff against master: `git diff master...$0`

**If nothing is given:**
- Use current branch: `git diff master...HEAD`
- Get commit messages: `git log --oneline master..HEAD`

## Title

Plain description. No prefix conventions.

- Good: `Change default stderr_reaction to log_last for executable UDFs`
- Good: `Fix crash when inserting NULL into non-nullable column`
- Good: `Fuzzer fixes`
- Bad: `fix(udf): Change default stderr_reaction to log_last` -- conventional commits = instant AI tell
- Bad: `Improve error handling and enrich exit code exceptions with stderr context` -- too polished

Keep it under 80 chars. Lowercase after first word is fine.

## Body

**Only the official template from `.github/PULL_REQUEST_TEMPLATE.md`. Nothing else above or below unless you have a genuine reason.**

Read `.github/PULL_REQUEST_TEMPLATE.md` for the exact template. Fill in the changelog category (delete the rest), write the changelog entry, and keep the documentation checkbox.

**If context is needed** (complex bug fix, behavioral change), add 1-3 paragraphs of plain text ABOVE the template. Free-form, no headers, no formatting. Like explaining to a colleague:

```
The previous default `throw` for stderr_reaction caused confusing errors when
UDFs wrote warnings to stderr but exited 0. Changed to `log_last` which matches
what most users expect. The `throw` behavior is still available via config.

### Changelog category (leave one):
- Bug Fix (user-visible misbehavior in an official stable release)

### Changelog entry:
Change default `stderr_reaction` from `throw` to `log_last` for executable UDFs.
Previously, UDFs that wrote warnings to stderr would fail even with exit code 0.
Fixes #XXXXX.

### Documentation entry for user-facing changes
- [ ] Documentation is written (mandatory for new features)
```

## Changelog entry guidelines

- Written for **users**, not developers
- Present tense: "Fix" not "Fixed"
- Say what changed AND why it matters to users
- Code elements in backticks
- Reference issue: `Fixes #XXXXX`
- 1-3 sentences max

## AI Slop Blacklist

These patterns are **never allowed** in ClickHouse PRs:

| Pattern | Why it's slop |
|---------|---------------|
| `fix(scope):` / `feat():` / `chore():` | Conventional commits -- ClickHouse doesn't use them |
| `## Summary` | Not in template, screams AI |
| `## Test plan` / `## Testing` | Tests speak for themselves |
| `## Changes` / `## What changed` | Not in template |
| `## Behavior after this change` | Not a ClickHouse convention |
| Markdown tables comparing behavior | Over-engineered, nobody does this |
| Checkbox lists `- [ ] tested X` | Not a ClickHouse convention |
| `This PR ...` to start the description | AI opener. Just describe the change directly |
| Bullet lists of file-by-file changes | Implementation details don't belong here |
| `## Motivation` as a header | If needed, just write it as plain text |
| Emojis in any form | No |
| `Co-Authored-By: Claude/Cursor/...` in PR descriptions | AI attribution in PR body is noise; commit trailers are a separate policy |
| Perfectly parallel sentence structures | Vary your phrasing |
| Words: "enhance", "streamline", "leverage", "robust", "comprehensive" | Classic AI vocabulary |

## For CI/not-for-changelog PRs

Minimal is best:

```
### Changelog category (leave one):
- Not for changelog (changelog entry is not required)

### Changelog entry (a user-readable short description of the changes that goes into CHANGELOG.md):

...

### Documentation entry for user-facing changes

- [ ] Documentation is written (mandatory for new features)
```

## Process

1. Read the diff to understand the change
2. Pick the right changelog category
3. Write a changelog entry for users (not developers)
4. Only add free-form context above template if the change is non-obvious
5. Re-read the body -- delete anything that a ClickHouse maintainer would call slop
6. Check title -- no conventional commit prefix, no AI-polished phrasing
