---
name: investigate-ci
description: Investigate a ClickHouse CI failure end-to-end from a PR or S3 report URL. Fetches the failed tests and their output, classifies each as flaky vs a real regression using play.clickhouse.com master history, and for every failure searches for both an existing tracking GitHub issue and an existing fix (open/merged PR) — reporting, per failure, whether an issue still needs to be created and whether a fix exists with its status (WIP, merged, already in this branch or not). Downloads and reads the harness artifacts only for failures that history does not explain, and reports a root-cause hypothesis. Read-only first pass — never commits, pushes, or edits.
argument-hint: "<PR-url | S3-report-url | issue-url> [threshold-days]"
disable-model-invocation: false
allowed-tools: Bash, Read, Grep, Glob, Agent, Task, WebFetch
---

# Investigate CI Failure Skill

A read-only first pass over a CI failure: turn a single URL into a per-test verdict
(flaky vs real) plus a root-cause hypothesis, with no copy-paste and no manual `wget`.

## Arguments

- `$0` (required): one of
  - a GitHub PR URL (`https://github.com/ClickHouse/ClickHouse/pull/NNNNN`),
  - a direct S3/CI report URL (`https://s3.amazonaws.com/.../json.html?PR=...&sha=...`), or
  - a GitHub **issue** URL (`https://github.com/ClickHouse/ClickHouse/issues/NNNNN`) — typically
    a bot-generated `flaky test` issue. Resolved to its report URL in step 0.
- `$1` (optional): master-history window in days for the flaky-vs-real verdict. Default `14`.

## Hard rules

- **Read-only.** Never `git commit`, `git push`, edit source, switch branches, close issues, or
  comment on the PR. This skill diagnoses; the human decides what to do. Surface findings, do not
  act on them. To read source at the report's commit, use `git show <sha>:<path>` rather than
  checking it out; only fetch/switch after asking the user (see step 4).
- Use `tmp/investigate/` for all working files, never `/tmp` (per CLAUDE.md).
- Wrap test names, identifiers, and log excerpts in backticks per the project style rule.
- Say "exception", not "crash", for logical errors.

## Steps

### 0. Resolve an issue URL to a report URL

`fetch_ci_report.js` accepts only PR, S3 `json.html`, and direct `result_*.json` URLs — **not**
issue links. If `$0` is `.../issues/NNNNN`, read the issue body and extract the report URL first:

```bash
gh issue view <NNNNN> --repo ClickHouse/ClickHouse --json title,body
```

Read the issue from the command output — do **not** redirect to a file. A
`gh issue view … > tmp/investigate/issue.json` redirect is a file write that rides the wildcard
`Bash(gh issue view:*)` allow (not the hook), so a symlinked `tmp`/`tmp/investigate` could land it
outside the scratch dir without a prompt. (Step 1 creates `tmp/investigate`.)

Bot-generated `flaky test` issues use this body format:

```
Test name: <test_name>
Failure reason: <reason>
CI report: <S3 json.html url>          ← use this as the report URL for the steps below
Failing test history: <play.clickhouse.com link>
```

Extract the `CI report:` URL and use it as `$0` for the rest of the skill. Also keep the
`Test name:` value — for a flaky-test issue the test to classify is already named, so step 3 can
run even if the S3 artifacts have expired (CI report links are retained only for a limited time;
if the report 404s, fall back to the named test plus the issue's `Failing test history` link).
If the issue is **not** a flaky-test report (no `CI report:` line), ask the user for the report or
PR URL rather than guessing.

**When the input is a flaky-test issue, step 3 is the decisive signal — run it first.** The
issue already names the test and the bot only files it after a failure, so master history settles
flaky-vs-real directly. Treat step 1 (the S3 report) and the optional step-4 artifact download as
best-effort enrichment: bot-filed issues are often weeks old and their artifacts have already
expired, so do not block on them.

### 1. Set up and fetch the failed tests

`fetch_ci_report.js` needs `node` on `PATH`. Run these as **separate** commands (not one compound
block) so each matches an allowed shape under the investigate profile — a combined
`mkdir … ; if … node …` string matches neither the exact `mkdir` allow nor the node-fetch hook and
would prompt. Primary inputs (PR/S3) skip step 0, so create the working dir here first:

```bash
mkdir -p tmp/investigate
```

Probe for `node` (`command -v` is allowed; do not wrap the fetch in an `if`/`;` one-liner, which
would hide the `node` command from the hook):

```bash
command -v node
```

If `node` is present, fetch the failed tests and their output:

```bash
node .claude/tools/fetch_ci_report.js "$0" --failed --cidb 2>&1
```

This prints the failed tests **and their output** straight from the praktika `result_*.json` (no
copy-paste), with a CIDB link per failed test. Read it from the command output — do **not** add a
`> tmp/investigate/…` redirect (a redirect is a file write the hook won't auto-approve, since it
can't be made symlink-safe, so it would prompt); the harness persists large output to a file you
can re-read or `grep`. If `node` is **absent**, skip the report fetch (and the step-4 download)
and rely on the issue body's failure output plus step 3 — do not treat a missing `node` as a
fatal error.

- If `$0` is a PR URL with many reports and the noise is high, narrow with `--report <n>`
  after listing reports (run the tool with no `--failed` to see the index).
- Record the PR number and the exact failed **test names** as they appear — the names must
  match `checks.test_name` for step 3 (and feed the issue search in step 2).
- Record the **count** of failed tests. The cheap steps (2–3) always run over all of them, but
  a large count changes how step 3 scopes the expensive deep-dive — see "Scope the deep-dive".

### 2. Search for an existing tracking issue and an existing fix

For **every** failed test, run two searches and record a per-test answer to two questions that go
into the final report:

- **Issue:** is the failure already tracked, or does an issue still need to be created?
- **Fix:** does a fix already exist, and what is its status (WIP / merged / already in this branch
  or not)?

Do both before the deep-dive — a tracked failure with a merged fix often short-circuits the rest
of the investigation.

#### 2a. Existing tracking issue + "does an issue need to be created?"

Search issues (open **and** closed) by test name — a hit often names the tracking flaky-test issue,
and its comments may already carry the root cause, a fix PR, or a "known flaky" note.

```bash
gh issue list --repo ClickHouse/ClickHouse --state all --limit 10 \
  --search "<distinctive test-name fragment> in:title,body" \
  --json number,title,state,stateReason,url,labels
```

Search on a distinctive fragment (the function or `test_*` name **without** the parametrization
suffix), not the full parametrized string — GitHub search tokenizes on punctuation and the full
name rarely matches. If a candidate looks relevant, read it **with its comments** — they often
already provide the diagnosis:

```bash
gh issue view <NNNNN> --repo ClickHouse/ClickHouse \
  --json number,title,state,stateReason,body,comments,labels
```

**How CI decides a failure is already tracked** (so you can answer "needs an issue?" the same way
CI's matcher does — see `ci/praktika/issue.py`, `Issue._check_flaky_test_match`): CI builds a
catalog from issues labeled **`testing`** (`IssueLabels.CI_ISSUE`) that are **open, or were closed
within the last ~8 hours**. A failure matches a catalog issue when:

- the issue's `Test name:` body field is a **suffix** of the failing test's name
  (`result.name.endswith(test_name)`; pytest parametrization/module rules apply), **and**
- if the issue sets a `Failure reason:`, that exact text is a **substring** of the failure output.

A matched failure is flagged with an `issue` label in the CI report — visible in the
`fetch_ci_report.js` output — which is the fastest "already tracked" signal.

**First, decide whether the per-test issue search even applies.** Some failures are not a single
test with one cause but a **generic, harness-level failure bucket** that aggregates many unrelated
causes: check-level verdicts like `Server died`, `Hung check failed, possible deadlock found`, or
the upgrade `Error message in clickhouse-server.log` check, and anonymized error *classes* like
`Logical error: Bad cast from type A to B` (the harness replaces concrete types with `A`/`B` and
groups by stack hash `STID`). Filing a per-failure tracking issue for these makes no sense — they
recur fleet-wide for shifting reasons. For such a bucket, **skip the issue search**: do not assert
`tracked`/`needs issue`, and record the Issue value as **`generic failure / untracked`**. Step 3's
master/cross-PR frequency is what establishes these are pre-existing and not caused by the PR; that
is the relevant signal, not a tracking issue. Run the per-test search below only for failures that
name a **specific** test (a `NNNNN_*`/`test_*` case) or a **specific, identifiable** crash/race
(e.g. a data race with a stable `STID` that maps to one code site).

Then determine, per test:

- **Generic failure / untracked** — a harness-level bucket or anonymized error class as above. No
  issue search run, no issue to file; rely on the step-3 frequency for the verdict.
- **Tracked** — an open (or just-closed) `testing` issue matches by the rule above (or the report
  already carries the `issue` label). No new issue needed; CI will keep auto-matching it.
- **Needs an issue** — the failure is a pre-existing **FLAKY** or **INFRA/BUILD** problem (per
  step 3), names a specific test/crash, and has **no** matching `testing` issue. Flag it as "issue
  needed" in the report.
- **No issue (fix instead)** — a **REAL** regression introduced by this PR. Do **not** flag it for
  a tracking issue: a `testing` issue would mask a real bug. The recommendation is to fix the code.
- **Stale/closed match** — only a closed issue matches, and it was closed more than ~8 h ago. CI
  no longer auto-matches it, so if the test is still flaky a fresh issue (or reopening) is needed;
  note the old issue number.

Never label a cell with an asserted fact you did not verify (e.g. "(known)" implying a tracking
issue exists when you ran no search). If you did not search, the value is `generic failure /
untracked`, not a tracking claim.

Record the matching issue number, its state (open vs closed/`completed`), labels, and any root
cause or fix PR mentioned. A closed `completed` issue whose fix post-dates the failing run points
straight at "retry, already fixed". If the input was itself an issue (step 0) you already have it,
but still scan for duplicate or related issues and read its comments.

#### 2b. Existing fix + its status

Independently of whether an issue exists, search for a **fix** — a PR that addresses this failure.
Three complementary sources, cheapest first:

- **From the tracking issue:** a fix PR is usually linked from the issue. Read its timeline for
  cross-referencing PRs (a `Closes #<issue>` in a PR shows up here):

  ```bash
  gh issue view <issue-number> --repo ClickHouse/ClickHouse --json number,state,stateReason,closedByPullRequestsReferences,timelineItems
  ```

- **By test name:** search PRs (open **and** merged) whose title/body names the test or its
  fragment:

  ```bash
  gh pr list --repo ClickHouse/ClickHouse --state all --limit 20 \
    --search "<distinctive test-name fragment>" \
    --json number,title,state,isDraft,mergedAt,mergeCommit,headRefName,url
  ```

- **By symptom:** for a `REAL`/`UNCERTAIN` failure, once step 5 names the suspect `file:line`,
  search PRs touching that file or the error string the same way.

For each candidate fix PR, classify its **status** — this is what goes in the report's Fix column:

- **WIP** — open PR. Note draft vs in-review (`isDraft`). Not yet protecting any run.
- **Merged** — `state == MERGED`, with a `mergeCommit`. Then decide **whether it is already in the
  failing run**, which determines the recommendation:
  - **Merged, NOT in this branch** → the fix landed on master after the report's commit. The PR
    just needs a rebase/retry. This is the common "already fixed on master — rebase and retry"
    case.
  - **Merged, ALREADY in this branch** → the fix was present in the failing run yet the test still
    failed. The "fix" is incomplete or unrelated — do **not** treat the failure as resolved; keep
    investigating (step 5).
- **None** — no fix PR found.

**Deciding "already in this branch or not".** Compare the fix's merge against the report commit
(`SHA` from step 1). If the fix's merge commit is in the local object store, ancestry is exact:

```bash
git cat-file -e <mergeCommit> && git merge-base --is-ancestor <mergeCommit> <report-sha> \
  && echo "fix IS in the failing run" || echo "fix is NOT in the failing run (or commit absent locally)"
```

If the merge commit is absent locally (`git cat-file -e` fails), fall back to time order: a fix
with `mergedAt` **after** the run's commit/`check_start_time` cannot be in the failing run →
"merged, not in this branch → rebase/retry". Surface that you used the time-order fallback rather
than exact ancestry.

### 3. Flaky-vs-real: query master history

This is the cheap, decisive triage — run it **before** downloading any artifacts (step 4),
since a `FLAKY` verdict usually makes the heavy download unnecessary.

Run **one** batch query against `play.clickhouse.com` for all failed test names. The `checks`
table is publicly readable via the `play` user. Adapt the threshold to `$1` (default `14`).

**Guard the empty case:** if no test names were extracted, `test_name IN ()` is invalid SQL —
skip and report "no named tests to classify" (the failure may be a build/infra error; go to
steps 4–5 to pull and read the build log).

**Escape single quotes** in every test name (`s/'/''/g`) before joining — parametrized
integration tests like `test_foo[a'b]` otherwise produce invalid SQL.

```bash
curl -sS 'https://play.clickhouse.com/?user=play' --data-binary "
SELECT test_name,
       countIf(test_status IN ('FAIL','ERROR') AND check_start_time >= now() - INTERVAL 90 DAY) AS fail_90d,
       countIf(test_status IN ('FAIL','ERROR') AND check_start_time >= now() - INTERVAL 30 DAY) AS fail_30d,
       countIf(test_status IN ('FAIL','ERROR') AND check_start_time >= now() - INTERVAL 14 DAY) AS fail_14d,
       countIf(test_status IN ('FAIL','ERROR') AND check_start_time >= now() - INTERVAL 7 DAY)  AS fail_7d,
       maxIf(check_start_time, test_status IN ('FAIL','ERROR'))                                  AS last_fail
FROM checks
WHERE check_start_time >= now() - INTERVAL 90 DAY
  AND test_name IN ( '<test1>', '<test2>', ... )
  AND pull_request_number = 0 AND head_ref = 'master'
GROUP BY test_name
ORDER BY fail_7d DESC
FORMAT TabSeparatedWithNames
"
```

**Gate on direct `master` rows only — `pull_request_number = 0 AND head_ref = 'master'`.**
`pull_request_number = 0` alone is not enough: it also matches release branches
(`head_ref = '26.5'`) and merge-queue refs (`head_ref = 'gh-readonly-queue/master/pr-...'`), so a
failure from an unrelated merge-queue PR or a release branch would inflate `fail_<window>d`.
And do *not* widen to `base_ref IN ('master','')`: that matches every PR targeting master, so any
unrelated PR's failure — including a real regression that PR introduced — would make
`fail_<window>d >= 1` and send the investigated PR's own regression down the `FLAKY` path,
skipping root-cause analysis. Only direct `master` HEAD rows show the test is flaky independent of
any PR. (Add `AND head_repo = 'ClickHouse/ClickHouse'` if fork rows are a concern.)

**Cross-PR failures are a separate, secondary signal.** If master rows are sparse, you may widen
to other PRs as *corroboration* — never as the gate — and only after comparing failure modes (a
flaky repeats the *same* error across unrelated PRs; distinct errors mean distinct bugs). Exclude
the investigated PR so its own failures never feed back in:

```sql
  AND base_ref IN ('master','') AND pull_request_number NOT IN (0, <investigated PR>)
```

**Never `GROUP BY` over or filter with `LIKE` on `test_context_raw`** — that column holds the
full test output and a 90-day scan over it times out (60 s limit). The aggregate query above is
safe because it touches only `test_status`/`check_start_time`. When you need per-failure detail
(dates, PRs, failure mode), the failing rows are few — **select them directly** and only
`substring(test_context_raw, 1, 200)` for a preview:

```bash
curl -sS 'https://play.clickhouse.com/?user=play' --data-binary "
SELECT toStartOfDay(check_start_time) AS day, pull_request_number AS pr, check_name,
       substring(test_context_raw, 1, 200) AS reason_head
FROM checks
WHERE check_start_time >= now() - INTERVAL 90 DAY
  AND test_name = '<test>'
  AND test_status IN ('FAIL','ERROR')
  AND pull_request_number = 0 AND head_ref = 'master'
ORDER BY day DESC
FORMAT TabSeparatedWithNames
"
```

Use this to confirm the master failures share the **same failure mode** as the report, and to
see which `check_name` configs are affected (e.g. only the heavily-loaded `arm_binary, parallel`
shard points to a load/timing race). In the optional cross-PR corroboration query, the same-error
test is the giveaway: a flaky fails identically across *unrelated* PRs, whereas distinct errors
mean distinct bugs.

**Classify each failed test:**

- `fail_<window>d >= 1` on master → **likely FLAKY** (pre-existing instability, not caused by
  this PR). Note the recent frequency.
- `0` rows returned, or `fail_90d == 0` on the master gate → **does not fail on direct master**.
  The gate is tight (`head_ref = 'master'` only) and master runs are far less frequent than PR
  runs, so a low-rate *fleet-wide* flaky can legitimately show `0` here. **Do not jump to REAL —
  run the cross-PR corroboration query first**, then:
  - Fails across multiple *unrelated* PRs with the same error → **FLAKY** (low rate; it just
    rarely lands on a direct-master run), not a regression.
  - The PR *adds* this test (`gh pr diff` shows the test file as new) → new test, judge on its
    own output, not history.
  - Absent on master **and** across other PRs, and the test already exists on master →
    **likely a REAL regression introduced by this PR**.
- Borderline (rare master failures, e.g. `fail_90d` small but `fail_14d == 0`) → **uncertain**;
  rely more heavily on the step-5 root-cause read.

Cross-check the verdict against the issue found in step 2: a known tracking issue corroborates a
**FLAKY** verdict (and may already give the root cause), while no issue plus no failures on master
*or* across other PRs strengthens **REAL**.

Always keep the per-test **CIDB link** from step 1 in the final report for manual drill-down.

Tests classified **FLAKY** need no artifacts — go straight to the report. Only **REAL**,
**UNCERTAIN**, or **INFRA/BUILD** failures need the step-4 download.

**Scope the deep-dive (many failures).** Steps 2–3 are cheap and always classify *every* failed
test — never silently drop any. The classification itself is the first filter: it usually leaves
only a handful of non-FLAKY tests, and steps 4–5 deep-dive only those. Before fanning out, check
two things:

- **Shared root cause?** When many *unrelated* tests fail with the **same** proximate error
  (server won't start, a build/link error, an early-setup or infra failure), they almost always
  share one cause. Investigate it **once**, not per-test — group them in the report under a single
  hypothesis. The step-3 per-failure detail query (failure-mode preview) is how you spot this.
- **Still many independent failures?** If more than a handful (~5) of genuinely distinct
  non-FLAKY failures remain, do **not** auto-spawn a subagent per test. List them with their
  one-line classifications and **ask the user which to investigate** (e.g. "the test this PR adds",
  "the 3 REAL ones", or a named subset). Report the full classification table regardless; only the
  expensive root-cause read is gated on the user's choice.

### 4. Download the harness artifacts (only as needed)

Download applies **only to the tests step 3 left as `REAL`, `UNCERTAIN`, or `INFRA/BUILD`**. If
every failed test is `FLAKY`, **skip this step entirely**.

**Read the error and the source before downloading anything.** The step-1 failure output usually
already contains the decisive evidence — an assertion message, an exception, a result diff, or a
stack trace. Read it, then open the referenced code (the stack-trace `file:line`, and
`gh pr diff <PR>` for the suspect change). For a large category of failures —
logical-error / assertion aborts with a symbolized stack, exceptions with a clear message, simple
stateless-test result diffs — that is enough to root-cause, and **no artifacts need to be
downloaded at all** (this investigation root-caused a `KeeperStateMachine.cpp` assertion straight
from the stack trace plus the source at the report commit).

**Read the source at the report's commit, or `file:line` will be wrong.** Stack-trace line
numbers are only accurate against the exact commit that produced the report. Step 1 prints the
report `SHA`; compare it to the checkout before trusting any line number:

```bash
git rev-parse HEAD    # compare against the report SHA from step 1
```

- **Match** → read the local files directly.
- **Differ, but the report commit is in the local object store** (`git cat-file -e <sha>` succeeds)
  → read the exact version without switching, e.g. `git show <sha>:src/path/File.cpp` and look
  around the reported line. This is read-only and leaves the working tree untouched — prefer it.
- **Differ and the commit is absent locally** → the `file:line` cannot be resolved. **Ask the
  user** whether to fetch and switch to the PR branch/commit; do not fetch or switch unilaterally
  (switching mutates the working tree, and the read-only investigate profile denies
  `git checkout`/`git switch`).

Download **only when reading the error and source leaves a real gap** — e.g. you need server
logs to see ordering/timing across nodes, the actual values behind a truncated diff, a core dump,
or the build log for an `INFRA/BUILD` failure. Then pull **only the specific files** that close
the gap, not the whole bundle reflexively (it is large and sometimes truncates).

`--download-logs <path>` takes a **file path** (not a directory) and downloads the single
`logs.tar.gz`/`logs.tar.zst` bundle to it — this replaces hand-supplying a base dir. It works
only against a **single concrete report** (an S3 `json.html`/`result_*.json` URL). A bare PR URL
takes the multi-report path and returns after the summary **without downloading**, so substitute
`<report-url>` — the S3 report URL for the failing check, or narrow the PR with `--report <n>`
first (list reports by running the tool with no `--failed`). This writes a file, so the hook does
**not** auto-approve it — it prompts under the investigate profile; approve it (this is the one
expected write, and it's the rare artifact-needed path):

```bash
node .claude/tools/fetch_ci_report.js "<report-url>" --failed --download-logs tmp/investigate/ci_logs.tar.gz
```

The tool prints the saved path and lists the archive's pytest logs. The compression may be zstd
despite the `.gz` name; extract with auto-detection (`-xf`, not `-xzf`). **Do not swallow the
`tar` error** — a failed extraction (expired/corrupt bundle, no `zstd` support, or the member
absent) is itself a finding; surface it instead of letting the later `grep | jq` silently yield
nothing and look "inconclusive":

```bash
tar -xf tmp/investigate/ci_logs.tar.gz -C tmp/investigate/ ci/tmp/pytest_parallel.jsonl
test -f tmp/investigate/ci/tmp/pytest_parallel.jsonl \
  || { echo "extraction FAILED — report the artifact problem (bundle expired/corrupt, missing zstd, or member absent), do not proceed as inconclusive"; false; }
```

For other artifacts (server logs, core dumps, query masks), list them with `--links` and pull
the specific URLs you need — `--download-logs` only fetches the one logs bundle.

For an integration-test failure, pull the relevant longrepr:

```bash
grep -F -- "<test_name>" tmp/investigate/ci/tmp/pytest_parallel.jsonl \
  | jq -r 'select((.longrepr // "") != "") | .longrepr'
```

Use `grep -F --` (fixed-string): parametrized names like `test_foo[a]` contain regex
metacharacters that a basic-regex `grep` would not match literally. (`jq` is allowed by the
locked-down profile; avoid `python3 -c`, which is not.)

### 5. Root-cause read (subagents)

If reading the error and source in step 4 already settled the root cause, just write it up — this
step is only for failures that still need digging. Dig **only into the failures step 3 scoped for
deep-dive** — the non-FLAKY ones the user confirmed, or a single shared-root-cause group — not
every failed test. When delegating, and especially when there are artifacts to wade through, use
subagents that return a concise summary, not raw dumps (per CLAUDE.md). Launch one subagent per
selected failure (or per shared-cause group) in parallel. Give each subagent:

- the failure output from step 1,
- the path(s) to any artifacts downloaded in step 4 (omit if none were needed),
- the PR diff for cross-referencing — run `gh pr diff <PR>` and pass its output (or have the
  subagent run it); do **not** redirect to a file (same symlink-write reason as step 0),
- the report `SHA`, instructing it to read source at that commit (`git show <sha>:<path>`) for
  accurate `file:line`, per step 4.

Ask each subagent to return: the proximate failure (assertion / exception / diff), the likely
root cause with `file:line` evidence from the logs **or the source**, and — if it looks real —
the suspect change in the PR diff. Tell it to read only; it must not modify anything.

### 6. Report

Print one verdict table, then a short narrative per real/uncertain failure:

| Test | Verdict | Master freq (7/14/30/90d) | Last master fail | Issue | Fix | Root-cause hypothesis | Suspect | CIDB |
|------|---------|---------------------------|------------------|-------|-----|-----------------------|---------|------|

- **Verdict** ∈ {`FLAKY`, `REAL`, `UNCERTAIN`, `NEW-TEST`, `INFRA/BUILD`}.
- **Issue** (from step 2a) ∈ {`generic failure / untracked` (harness-level bucket or anonymized
  error class — no issue search run, none to file), `tracked #N`, `needs issue`, `fix instead`
  (REAL — don't mask), `stale #N` (closed >~8 h ago)}. Never write a tracking claim like "(known)"
  for a failure you did not search.
- **Fix** (from step 2b) ∈ {`none`, `WIP #N` (open; note draft), `merged #N — in branch` (fix was
  present yet test still failed → keep digging), `merged #N — rebase/retry` (landed after the run)}.
- For `REAL`/`UNCERTAIN`, give the `file:line` evidence and the suspect PR change.
- For `FLAKY`, state the master frequency and the tracking flaky-test issue from step 2 (number,
  state, and any root cause / fix PR its comments revealed).
- End with a one-line recommendation per test that combines verdict, issue, and fix, e.g.:
  - "retry — flaky on master, tracked by #N";
  - "retry — already fixed on master by #N (merged after this run), rebase";
  - "flaky on master, **no tracking issue — create one**";
  - "real regression in `<file>`, see `<commit>` — fix the PR, do not file a tracking issue";
  - "merged fix #N is already in this branch but the test still failed — needs manual look".

Include the original report URL (and PR link) so the human can confirm. Do not take any action.
