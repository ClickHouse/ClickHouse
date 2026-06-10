---
name: investigate-ci
description: Investigate a ClickHouse CI failure end-to-end from a PR or S3 report URL. Fetches the failed tests and their output, searches for an existing tracking GitHub issue, classifies each as flaky vs a real regression using play.clickhouse.com master history, downloads and reads the harness artifacts only for failures that history does not explain, and reports a root-cause hypothesis. Read-only first pass — never commits, pushes, or edits.
argument-hint: "<PR-url | S3-report-url | issue-url> [threshold-days]"
disable-model-invocation: false
allowed-tools: Bash, Read, Grep, Glob, Agent, WebFetch
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
mkdir -p tmp/investigate
gh issue view <NNNNN> --repo ClickHouse/ClickHouse --json title,body > tmp/investigate/issue.json
```

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

`fetch_ci_report.js` needs `node` on `PATH`. Check once; if absent, skip the report fetch (and
the step-4 download) and rely on the issue body's failure output plus step 3 (do not treat a
missing `node` as a fatal error):

```bash
mkdir -p tmp/investigate   # primary inputs (PR/S3) skip step 0, so create it here before the redirect
if command -v node >/dev/null; then
  node .claude/tools/fetch_ci_report.js "$0" --failed --cidb > tmp/investigate/failed.txt 2>&1
else
  echo "node not available — skipping fetch_ci_report.js, using step 3"
fi
```

When the `node` branch ran, this pulls the failed tests **and their output** straight from the
praktika `result_*.json` (no copy-paste) and prints a CIDB link per failed test; read
`tmp/investigate/failed.txt`. When `node` was absent, `failed.txt` does not exist — skip reading
it and rely on the issue body's failure output plus step 3.

- If `$0` is a PR URL with many reports and the noise is high, narrow with `--report <n>`
  after listing reports (run the tool with no `--failed` to see the index).
- Record the PR number and the exact failed **test names** as they appear — the names must
  match `checks.test_name` for step 3 (and feed the issue search in step 2).
- Record the **count** of failed tests. The cheap steps (2–3) always run over all of them, but
  a large count changes how step 3 scopes the expensive deep-dive — see "Scope the deep-dive".

### 2. Search for an existing GitHub issue

Before querying history, check whether the failure is already tracked. For each failed test,
search issues (open **and** closed) by test name — a hit often names the tracking flaky-test
issue, and its comments may already carry the root cause, a fix PR, or a "known flaky" note that
short-circuits the rest of the investigation.

```bash
gh issue list --repo ClickHouse/ClickHouse --state all --limit 10 \
  --search "<distinctive test-name fragment> in:title,body" \
  --json number,title,state,stateReason,url
```

Search on a distinctive fragment (the function or `test_*` name **without** the parametrization
suffix), not the full parametrized string — GitHub search tokenizes on punctuation and the full
name rarely matches. If a candidate looks relevant, read it **with its comments** — they often
already provide the diagnosis:

```bash
gh issue view <NNNNN> --repo ClickHouse/ClickHouse \
  --json number,title,state,stateReason,body,comments
```

Record the matching issue number, its state (open vs closed/`completed`), and any root cause or
fix PR mentioned — feed it into the step-3 classification and the final report. A closed
`completed` issue whose fix post-dates the failing run points straight at "retry, already fixed".
If the input was itself an issue (step 0) you already have it, but still scan for duplicate or
related issues and read its comments.

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
  AND (pull_request_number = 0 OR base_ref IN ('master',''))
  AND pull_request_number != <investigated PR>
GROUP BY test_name
ORDER BY fail_7d DESC
FORMAT TabSeparatedWithNames
"
```

The `pull_request_number = 0 OR base_ref IN ('master','')` filter restricts to master commits
and PRs targeting master, dropping fork-PR noise.

**Exclude the PR you are investigating** (`AND pull_request_number != <PR>`, the number from
step 1). Its own failing checks carry `base_ref='master'`, so without this clause they satisfy
the filter above and inflate `fail_<window>d` — a real regression that fails *only* in this PR
would then read as `fail_<window>d >= 1` and be misclassified `FLAKY`, skipping root-cause
analysis. The remaining rows (true master + *other* PRs) are exactly the independent signal the
flaky verdict needs. When investigating master directly or an issue with no associated PR, there
is no PR to exclude — drop the clause.

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
  AND (pull_request_number = 0 OR base_ref IN ('master',''))
  AND pull_request_number != <investigated PR>
ORDER BY day DESC
FORMAT TabSeparatedWithNames
"
```

Use this to confirm the master failures share the **same failure mode** as the report (a real
regression repeats the same error; a flaky test often fails identically across *unrelated* PRs)
and to see which `check_name` configs are affected (e.g. only the heavily-loaded
`arm_binary, parallel` shard points to a load/timing race).

**Classify each failed test:**

- `fail_<window>d >= 1` on master → **likely FLAKY** (pre-existing instability, not caused by
  this PR). Note the recent frequency.
- `0` rows returned, or `fail_90d == 0` → **never fails on master**. Two sub-cases:
  - The test already exists on master → **likely a REAL regression introduced by this PR**.
  - The PR *adds* this test (`gh pr diff` shows the test file as new) → new test, judge on its
    own output, not history.
- Borderline (rare master failures, e.g. `fail_90d` small but `fail_14d == 0`) → **uncertain**;
  rely more heavily on the step-5 root-cause read.

Cross-check the verdict against the issue found in step 2: a known tracking issue corroborates a
**FLAKY** verdict (and may already give the root cause), while no issue plus zero master history
strengthens **REAL**.

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
first (list reports by running the tool with no `--failed`):

```bash
node .claude/tools/fetch_ci_report.js "<report-url>" --failed --download-logs tmp/investigate/ci_logs.tar.gz
```

The tool prints the saved path and lists the archive's pytest logs. The compression may be zstd
despite the `.gz` name; extract with auto-detection (`-xf`, not `-xzf`):

```bash
tar -xf tmp/investigate/ci_logs.tar.gz -C tmp/investigate/ ci/tmp/pytest_parallel.jsonl 2>/dev/null || true
```

For other artifacts (server logs, core dumps, query masks), list them with `--links` and pull
the specific URLs you need — `--download-logs` only fetches the one logs bundle.

For an integration-test failure, pull the relevant longrepr:

```bash
grep "<test_name>" tmp/investigate/ci/tmp/pytest_parallel.jsonl \
  | jq -r 'select((.longrepr // "") != "") | .longrepr'
```

(`jq` is allowed by the locked-down profile; avoid `python3 -c`, which is not.)

### 5. Root-cause read (subagents)

If reading the error and source in step 4 already settled the root cause, just write it up — this
step is only for failures that still need digging. Dig **only into the failures step 3 scoped for
deep-dive** — the non-FLAKY ones the user confirmed, or a single shared-root-cause group — not
every failed test. When delegating, and especially when there are artifacts to wade through, use
subagents that return a concise summary, not raw dumps (per CLAUDE.md). Launch one subagent per
selected failure (or per shared-cause group) in parallel. Give each subagent:

- the failure output from step 1,
- the path(s) to any artifacts downloaded in step 4 (omit if none were needed),
- the PR diff for cross-referencing (`gh pr diff <PR> > tmp/investigate/pr.diff`),
- the report `SHA`, instructing it to read source at that commit (`git show <sha>:<path>`) for
  accurate `file:line`, per step 4.

Ask each subagent to return: the proximate failure (assertion / exception / diff), the likely
root cause with `file:line` evidence from the logs **or the source**, and — if it looks real —
the suspect change in the PR diff. Tell it to read only; it must not modify anything.

### 6. Report

Print one verdict table, then a short narrative per real/uncertain failure:

| Test | Verdict | Master freq (7/14/30/90d) | Last master fail | Root-cause hypothesis | Suspect | CIDB |
|------|---------|---------------------------|------------------|-----------------------|---------|------|

- **Verdict** ∈ {`FLAKY`, `REAL`, `UNCERTAIN`, `NEW-TEST`, `INFRA/BUILD`}.
- For `REAL`/`UNCERTAIN`, give the `file:line` evidence and the suspect PR change.
- For `FLAKY`, state the master frequency and the tracking flaky-test issue from step 2 (number,
  state, and any root cause / fix PR its comments revealed).
- End with a one-line recommendation per test (e.g. "retry — flaky on master",
  "real regression in `<file>`, see `<commit>`", "needs manual look — logs inconclusive").

Include the original report URL (and PR link) so the human can confirm. Do not take any action.
