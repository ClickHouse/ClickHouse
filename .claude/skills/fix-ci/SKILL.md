---
name: fix-ci
description: Triage every failed check on ClickHouse master over a recent window (default 24 hours, or the query behind a play.clickhouse.com "Failed checks" link) and, for each kind of failure, either find the existing fix (merged or open PR, tracking issue) or make one — a separate branch and PR per new fix. Use when asked to "analyze all failures", "fix master CI", "fix each of them or find existing fixes", or given a play.clickhouse.com link listing master failures.
argument-hint: "[play.clickhouse.com URL | hours]"
disable-model-invocation: false
allowed-tools: Bash, Read, Edit, Write, Grep, Glob, Agent, Task
---

# Fix CI Skill

Turn "everything red on master in the last day" into a per-failure verdict: fixed already
(merged PR), fix in flight (open PR), tracked but blocked (issue awaiting a decision), or fixed
now (new PR from this run).

## Arguments

- `$0` (optional): either
  - a `play.clickhouse.com` URL. The SQL is base64 in the URL fragment (after `#`); decode it with
    `python3 -c 'import base64,sys,urllib.parse; print(base64.b64decode(urllib.parse.unquote(sys.argv[1].split("#",1)[1])).decode())' '<url>'`.
    The usual "Failed checks" link holds three queries (KPI, ppm, the failure list); run only the
    last one, the list of failures; or
  - a number of hours for the window. Default `24`.

## Rules

- Follow `CLAUDE.md`: a new branch per fix (never commit to `master`), no rebase/amend, Allman
  braces, `tmp/` for scratch files, PR body from `.github/PULL_REQUEST_TEMPLATE.md`, include the CI
  report URL in the commit message and the PR description.
- One PR per independent root cause. Do not bundle unrelated fixes.
- Never fix a failure by disabling or deleting the test, adding `no-*` tags, or adding sleeps.
  If the test catches a real bug, fix the code.
- Do not open a PR for a failure that already has a merged fix or an open PR; report the link.
- When a fix needs a decision between alternatives that change user-visible behaviour (packaging,
  removing features, changing defaults) and an issue already lays out the options, stop and ask
  the user which one, with a recommendation. Do not pick for them.
- Say "exception", not "crash", for logical errors.

## Steps

### 1. Get the list of failures

```bash
mkdir -p tmp/fix-ci
cat > tmp/fix-ci/failures.sql <<'EOF'
SELECT check_start_time, check_name, test_name, report_url, commit_sha
FROM checks
WHERE check_start_time >= now() - INTERVAL 24 HOUR
    AND head_ref = 'master' AND startsWith(head_repo, 'ClickHouse/')
    AND test_status != 'SKIPPED'
    AND (test_status LIKE 'F%' OR test_status LIKE 'E%')
    AND check_status != 'success'
ORDER BY check_start_time DESC, check_name, test_name
FORMAT TSV
EOF
curl -sS 'https://play.clickhouse.com/?user=play' --data-binary @tmp/fix-ci/failures.sql > tmp/fix-ci/failures.tsv
cut -f2,3 tmp/fix-ci/failures.tsv | sort | uniq -c | sort -rn
```

Group rows into **kinds**: the same `test_name` across shards or architectures is one kind; the
same failure on two consecutive commits is one kind. Sanitizer reports are named by STID
(`ThreadSanitizer: data race (STID: ...)`), and one STID covers many unrelated stacks, so group
those by the top frames of the report, not by name.

If an earlier run of this skill left notes (memory or a previous report), read them first and
check whether the PRs listed there have merged; many kinds will already be covered.

### 2. Classify each kind with history

For each kind, query play.clickhouse.com for history over the last 30 days, both on master and
on PRs:

```sql
SELECT toDate(check_start_time) d, head_ref = 'master' AS m,
       countIf(test_status IN ('FAIL', 'ERROR')) AS failed, count() AS total
FROM checks
WHERE test_name = '<test_name>' AND check_start_time > now() - INTERVAL 30 DAY
GROUP BY d, m ORDER BY d
```

- **Started at a specific commit** (fails on every commit from a point on): find the first bad
  commit by listing per-commit status for the check on master
  (`SELECT check_start_time, commit_sha, test_status ... WHERE head_ref = 'master' AND check_name = ...`),
  then `git log --oneline --first-parent <last_good>..<first_bad>` on a fresh
  `git fetch https://github.com/ClickHouse/ClickHouse.git master:refs/remotes/origin/master`.
  Check whether the suspect PR's own CI ran this check green; if so, the cause is an interaction or
  a threshold being crossed (sizes, timeouts), not the PR's code alone.
- **Rows with `pull_request_number = 0` but `head_ref != 'master'`** are release branches. A failure
  that started there first (e.g. `26.10`) points at gradual growth rather than one commit.
- **Intermittent** (about 1% of runs): flaky. Find the first day it appeared and the PRs merged
  around then; flakiness that starts on a date is usually a regression, not "infrastructure".

### 3. Investigate kinds in parallel

Spawn one subagent per kind that is not explained by step 2 (keep it under ~5 agents). Give each:
the check name, test name, report URL, commit SHA, the history found in step 2, and these
instructions:

- `node .claude/tools/fetch_ci_report.js "<report_url>" --failed --links`; download logs with
  `--download-logs <job tmp dir>` only if the failure text is not enough. For integration tests,
  extract `ci/tmp/pytest_parallel.jsonl` for `longrepr` and read the server log of the instance.
- For libFuzzer: get the crash kind (timeout, OOM, sanitizer, logical error) and compare the input
  prefix with inputs of recent fuzzer fix PRs; mutations of one corpus entry are the same bug.
- Search for existing issues and fixes: `gh issue list -R ClickHouse/ClickHouse --state all --search "<test or top frame>"`,
  `gh pr list -R ClickHouse/ClickHouse --state all --search "..."`, and `gh search issues` by the
  error text (e.g. `cpio Digest mismatch`). Include PRs merged after the failing commit.
- Read the source and give a root cause with `file:line`, and a concrete fix if none exists.
- Read-only: no commits, pushes or repository edits.

Investigate the newest and most widespread kind yourself while the agents run.

For packaging checks (`Install packages (...)`), the build job's links (`fetch_ci_report.js
"<build report>" --links`) list every package; fetch only the first ~256 KB of a package with
`curl -r 0-262143` to read its header (e.g. rpm `FILESIZES`, tag 1028) instead of downloading
gigabytes.

### 4. Fix what is not fixed

For each kind without a merged fix or open PR:

1. Branch from fresh `origin/master` (`git checkout -b <topic> origin/master` in your own worktree).
2. Make the minimal fix at the root cause. For a test that catches a real regression, fix the code
   and let the existing test cover it.
3. Verify: build and run the test if feasible; at minimum compile the changed translation unit with
   `-fsyntax-only` using the command from a build's `compile_commands.json`. Say which you did.
4. Commit with the CI report URL in the message; push over HTTPS
   (`git push https://github.com/ClickHouse/ClickHouse.git HEAD:refs/heads/<topic>`).
5. Open the PR from the template. Put `Closes: <issue>` if a tracking issue exists and
   `Related: <PR or issue>` lines for the regressing PR and related work (`Caused by` belongs in
   issues, not PRs). Category:
   - "CI Fix or Improvement" for test-only/CI-only fixes;
   - "Bug Fix" only when the misbehaviour is in a released version;
   - "Not for changelog" for a regression that has not reached a stable release yet.

For a kind that cannot be fixed in this run (needs a decision, not reproducible, external) and
has no tracking issue, file one with the CI report links and the history from step 2. Put
`Caused by: <PR>` in it when the regressing PR is known; otherwise add the
`v<major>.<minor>-affected` labels for the release branches that contain the cause, or say that
the origin is unknown.

If the regressing PR gets reverted while you work, the fix PR should re-land it: merge
`origin/master` into the fix branch, then `git revert <revert commit>`, and update the PR title,
description and changelog category to cover the re-landed change.

### 5. Report

End with a table, one row per kind: check / test, root cause (one line), and the status with a
link: fixed by merged PR, open PR (whose), new PR from this run, or issue awaiting a decision (with
the options and a recommendation). State what was verified for each new PR. Then list every
decision the user has to make.
