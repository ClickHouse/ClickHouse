---
name: good-prs
description: Show a report of open ClickHouse PRs whose only non-green CI check is "CH Inc sync" (or that are fully green) — i.e. effectively ready to merge. Groups by your authored PRs, PRs assigned to you (authored by others), and PRs by tracked authors (default groeneai). Shows the CH Inc sync state and whether each PR was ever approved; excludes already-merged PRs. Use when asked for "good PRs", merge-ready PRs, or PRs blocked only on the sync job.
argument-hint: "[tracked-author ...]   # default: groeneai"
disable-model-invocation: false
allowed-tools: Bash(bash:*), Bash(gh:*), Bash(jq:*), Bash(xargs:*), Bash(awk:*), Bash(grep:*), Bash(cut:*), Bash(sed:*), Read
---

# Good PRs Skill

Produce a report of open pull requests in `ClickHouse/ClickHouse` that are effectively
ready to merge: every CI check is green/skipped **except possibly `CH Inc sync`**, which
is the private-repo sync job and is frequently the last thing standing between a PR and
merge. Fully-green PRs are included too.

## What counts as a "good PR"

For each PR, look at all checks and ignore the aggregate gates (`PR`, `Mergeable Check`,
`A Sync (only for tests)`). A PR qualifies when **every remaining check other than
`CH Inc sync` is `SUCCESS`, `SKIPPED`, or `NEUTRAL`** (nothing else is failing or still
running). The `CH Inc sync` state is then reported as one of:

- **GREEN** — `CH Inc sync` also passed → fully green / merge-ready.
- **FAILED** — only `CH Inc sync` failed.
- **INPROG** — only `CH Inc sync` is still running (`PENDING`/`IN_PROGRESS`).
- **NOSYNC** — there is no `CH Inc sync` check at all and everything else is green
  (typically release-branch backports).

Already-merged or closed PRs are excluded. Each row also shows whether the PR was
**approved by anyone at least once** (any historical `APPROVED` review event; it does
*not* require the approval to still be current after later pushes).

## Sections

1. **Your own PRs** — authored by the authenticated `gh` user.
2. **Assigned to you, authored by others** — assigned to you, excluding your own PRs and
   the tracked authors (those get their own section).
3. **PRs by each tracked author** — every qualifying PR by the author, regardless of
   assignee. Default tracked author: `groeneai`. Pass author logins as arguments to
   change this (e.g. `good-prs groeneai azat`).

## How to run

Run the bundled script and present its Markdown output directly to the user (it already
emits finished tables):

```bash
bash .claude/skills/good-prs/report.sh $ARGUMENTS
```

The script fetches PR lists with `gh pr list`, classifies each PR's checks with
`gh pr checks`, and looks up state + approvals with `gh pr view`, all parallelized with
`xargs -P 12`. For groeneai-sized author sets (~200 open PRs) it takes roughly a minute.
Every `gh` call is pinned to `--repo ClickHouse/ClickHouse`, so the report is correct
regardless of the directory the skill is run from.

## Testing

`test.sh` runs `report.sh` against a stubbed `gh` (no network) and checks the
`bucket`→label classification, the "only `CH Inc sync` is non-green" criterion, empty
input, `--repo` pinning, the fail-loud-on-real-error behaviour, and the retry policy
(a transient failure is retried until it succeeds, a permanent one is not retried):

```bash
bash .claude/skills/good-prs/test.sh
```

## Presentation

- The script's stdout is a complete report — show it as-is.
- Statuses drift constantly as CI runs; if the user asks to "check again", just re-run
  the script. Mention it is a point-in-time snapshot.
- If the user wants only the failed/in-progress subset (not the fully-green ones), filter
  the rows to `FAILED`/`INPROG` after running, or note which rows are `GREEN`.

## Notes and caveats

- `gh pr list --author <login>` is backed by GitHub's search index, which can
  occasionally omit an individual PR (a stale-index gap). If a specific PR is known to be
  missing, fetch it explicitly with `gh pr view <n>` and add it.
- The aggregate gates excluded from the "everything else is green" test are `PR`,
  `Mergeable Check`, and `A Sync (only for tests)`. If ClickHouse CI renames or adds an
  aggregate gate, update the `select(...)` filter in `report.sh`.
- Checks are classified by `gh pr checks`' own `bucket` field (`pass` / `fail` /
  `pending` / `skipping` / `cancel`), not by raw state strings, so unusual states such as
  `QUEUED`, `TIMED_OUT`, `CANCELLED`, or `STARTUP_FAILURE` are handled without a PR being
  silently dropped.
- A full report makes roughly two GraphQL calls per PR — several hundred per run — so
  hitting at least one transient GitHub failure is close to certain. Those (`HTTP 408`,
  `429`, any `5xx`, gateway timeouts, connection resets, secondary rate limits) are
  retried with exponential backoff, and a primary rate-limit rejection waits for the
  hourly window to reset. `GH_RETRIES` (default 5) and `GH_RETRY_DELAY` (default 2
  seconds) tune this.
- The script still fails loudly: an error that will never fix itself (auth, unknown PR,
  bad flag) aborts on the first attempt, and a transient error that outlives every retry
  aborts too — in both cases with the original `gh` diagnostic, rather than silently
  omitting that PR from the report.
- A run costs a large share of the hourly GraphQL quota (5000 points), so a few
  back-to-back runs can exhaust it. Check with
  `gh api rate_limit --jq .resources.graphql`.
- The script needs `gh` authenticated and `jq` available.
