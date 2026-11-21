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
- **Run every `gh` read through `.claude/tools/gh-ro.sh` (same args as `gh`).** It drops a poisoned
  `GH_CONFIG_DIR` — some agent/CI runners set it to a config dir with no working auth, which makes
  raw `gh` fail — and refuses any non-read-only subcommand, so it can never create/close/edit/merge/
  comment. The examples below use it for this reason. (`fetch_ci_report.js` clears `GH_CONFIG_DIR`
  internally, so drive it as `node .claude/tools/fetch_ci_report.js …` directly.)
- Wrap test names, identifiers, and log excerpts in backticks per the project style rule.
- Say "exception", not "crash", for logical errors.

## Steps

### 0. Resolve an issue URL to a report URL

`fetch_ci_report.js` accepts only PR, S3 `json.html`, and direct `result_*.json` URLs — **not**
issue links. If `$0` is `.../issues/NNNNN`, read the issue body and extract the report URL first:

```bash
.claude/tools/gh-ro.sh issue view <NNNNN> --repo ClickHouse/ClickHouse --json title,body
```

Read the issue from the command output — do **not** redirect to a file. A
`.claude/tools/gh-ro.sh issue view … > tmp/investigate/issue.json` redirect is a file write that
rides the wildcard `Bash(.claude/tools/gh-ro.sh:*)` allow (not the hook), so a symlinked
`tmp`/`tmp/investigate` could land it outside the scratch dir without a prompt. (Step 1 creates
`tmp/investigate`.)

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
can re-read or `grep`. **If `node` is absent, the fallback depends on the input type:**

- **Issue URL** (step 0 already gave you `Test name:` and the `Failing test history` link) → proceed
  without the report: run step 3 on that named test; the S3 report and step-4 artifacts are
  best-effort enrichment. A missing `node` is not fatal here.
- **PR or S3 report URL** → `node` is **required**. Without the report you have no failed test
  names, job names, or labels, so steps 2–3 (issue/fix search and the `test_name IN (...)` history
  query) cannot run. Do **not** limp on with a partial investigation — stop and tell the user to
  install `node` (or re-run where `node` is on `PATH`).

Per failure the tool prints a `🏷️ labels:` line (CI's non-CIDB labels — the `issue` match link and
flags like `retry_ok`; see step 2a), the CIDB link, and the **log *tail*** — the last ~30 non-empty
lines of `result.info`, **not** the full output. CI's matchers test `Failure reason` against the
**whole** `result.info`, so a `Failure reason` located earlier than the tail will match in CI yet be
**invisible** here — when mirroring CI's attribution (step 2a) or judging whether a reason string is
present, do not read a "not in the visible output" as a definitive non-match; drill via the CIDB
link or the full artifact (step 4) if it matters.

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
.claude/tools/gh-ro.sh issue list --repo ClickHouse/ClickHouse --state all --limit 10 \
  --search "<distinctive test-name fragment> in:title,body" \
  --json number,title,state,stateReason,url,labels,closedAt
```

Search on a distinctive fragment (the function or `test_*` name **without** the parametrization
suffix), not the full parametrized string — GitHub search tokenizes on punctuation and the full
name rarely matches. If a candidate looks relevant, read it **with its comments** — they often
already provide the diagnosis:

```bash
.claude/tools/gh-ro.sh issue view <NNNNN> --repo ClickHouse/ClickHouse \
  --json number,title,state,stateReason,body,comments,labels,closedAt
```

**How CI decides a failure is already tracked** (so you can answer "needs an issue?" the same way
CI's matcher does — see `ci/praktika/issue.py`, `Issue.check_result`): CI builds a catalog from
issues labeled **`testing`** (`IssueLabels.CI_ISSUE`) that are **open, or were closed within the
last ~8 hours**, and routes each by whether it also carries the **`infrastructure`** label:

- **Flaky-test issues** (no `infrastructure` label) → `Issue._check_flaky_test_match`. Matches when
  the issue's `Test name:` body field is a **suffix** of the failing test's name
  (`result.name.endswith(test_name)`; pytest parametrization/module rules apply), **and** if the
  issue sets a `Failure reason:`, that text is a **substring** of the failure output.
- **Infrastructure issues** (`testing` **+** `infrastructure` label) → `Issue._check_infrastructure_match`.
  These do **not** match by `Test name:`. They match a failure when **all** of the present fields
  hold: `Failure reason:` is a substring of the output; every `Failure flags:` value (e.g.
  `retry_ok`) is a label on the result; `Test pattern:` matches the test name; and `Job pattern:`
  matches the **job** name. Note the pattern matching is **not** true SQL `LIKE`: the pattern is
  split on `%`, empty fragments are dropped, and it matches if **any** remaining fragment is a plain
  substring (so it is OR-across-fragments, order is not enforced, and a bare `%` — like an empty
  field — is treated as no constraint). Examples:
  [#87123](https://github.com/ClickHouse/ClickHouse/issues/87123) (`Job pattern: Unit%`),
  [#91410](https://github.com/ClickHouse/ClickHouse/issues/91410),
  [#92089](https://github.com/ClickHouse/ClickHouse/issues/92089) (`Job pattern: Stateless tests (amd_msan%`,
  `Failure reason: DB::Exception: Timeout exceeded`).

**Fast path — read the labels `fetch_ci_report.js` prints.** The tool surfaces each failure's
non-CIDB labels on a `🏷️ labels:` line. Two are decisive:

- An **`issue`** label gives you the matched issue number **for free** — the printed link is the
  issue CI matched **at run time** (e.g. `Server died` → `issue (…/issues/107487)`, `Hung check …`
  → `…/107941`). It saves the *search*, but it is **not** by itself `tracked #N`: the label reflects
  the catalog when the report was produced, not the issue's state **now**. Always `.claude/tools/gh-ro.sh issue view`
  the linked issue and classify from its **current** `state`/`closedAt` — an issue closed after the
  run and aged past the ~8 h window is `stale #N` (reopen candidate), not `tracked`. The label
  shortcuts the lookup; it does not replace the tracked-vs-stale decision below.
- **Failure flags** (e.g. `retry_ok`) appear here too — these are exactly the labels an
  infrastructure issue matches on via `Failure flags:` (below), so this line is how you verify that
  constraint.

And **absence** of an `issue` label is **not** proof of "untracked": CI stamps it from the catalog
*as it was at that run* (open + closed-within-8h then), so a tracking issue filed or reopened
**after** the run won't show. So: an `issue` label → look up that issue and classify by current
state; no label → still run the GitHub search below before concluding `needs issue`/`untracked`.

**For an `INFRA/BUILD` or timeout/harness-level failure, also run the infrastructure path.** A
test-name search alone will miss these, so a pre-existing, already-tracked infra failure would be
mis-reported as `needs issue`. List the infra issues — **`--state all`**, because CI's catalog
includes not just open ones but every `testing` issue closed in the last ~8 h
(`TestCaseIssueCatalog.from_gh`), and a just-closed infra issue is still auto-matched — and match
by `Job pattern` / `Failure reason` against the failing job and its output:

```bash
.claude/tools/gh-ro.sh issue list --repo ClickHouse/ClickHouse --state all --label testing --label infrastructure \
  --limit 100 --json number,title,url,body,state,closedAt
```

Compare each candidate's fields the way `_check_infrastructure_match` does: `Failure reason:` is a
substring of the output; `Job pattern:`/`Test pattern:` match the failing job/test name; and every
`Failure flags:` value must appear on the failure's `🏷️ labels:` line from step 1 (that line is
the only place these flags are visible — e.g. `test_dns_cache … → retry_ok`). All present fields
must hold. A match on an open issue — or one closed within ~8 h (check `closedAt`) — is `tracked #N`
exactly as CI would attribute it; a match on an issue closed longer ago is a `stale #N` reopen
candidate (same
same-failure/recurrence check as the flaky case).

**Always run the issue search for every failed name** — it is one cheap `gh issue list` and is the
only way to mirror CI's attribution. Generic, harness-level names get tracked too: `Server died`,
`Hung check failed, possible deadlock found`, and the upgrade `Error message in
clickhouse-server.log` check frequently *do* have a `testing` issue (e.g. `Server died` →
[#107487](https://github.com/ClickHouse/ClickHouse/issues/107487), `Test name: Server died`), and
`Issue._check_flaky_test_match` will mark such a result with the `issue` label. So do **not** skip
the search for them. The `generic failure / untracked` value is **only** for a generic bucket or
anonymized error *class* (e.g. `Logical error: Bad cast from type A to B`, where the harness
replaces concrete types with `A`/`B` and groups by stack hash `STID`) **after** the search finds no
matching `testing` issue — because filing a *new* per-failure issue for such a shifting bucket
makes no sense. It is a "searched, nothing matched, and not worth filing" verdict, never a
"didn't look" one.

Determine, per test:

- **Tracked** — a `testing` issue matches (by the rule above, or reached via the report's `issue`
  label) and is **currently** open or closed within ~8 h (`.claude/tools/gh-ro.sh issue view` → `state`/`closedAt`). No
  new issue needed; CI will keep auto-matching it. Applies to generic-bucket names too when such an
  issue exists (e.g. `Server died` → #107487). If the linked/ matched issue is closed longer ago,
  it is `stale #N`, not `tracked`.
- **Needs an issue** — the failure is a pre-existing **FLAKY** or **INFRA/BUILD** problem (per
  step 3), names a **specific** test/crash (a `NNNNN_*`/`test_*` case, or an identifiable crash/race
  with a stable `STID` mapping to one code site), and has **no** matching `testing` issue. Flag it
  as "issue needed" in the report.
- **Generic failure / untracked** — a generic harness bucket or anonymized error class for which
  the search found **no** matching `testing` issue, and filing a new per-failure issue makes no
  sense. Rely on the step-3 frequency for the verdict.
- **No issue (fix instead)** — a **REAL** regression introduced by this PR. Do **not** flag it for
  a tracking issue: a `testing` issue would mask a real bug. The recommendation is to fix the code.
- **Stale/closed match (reopen candidate)** — only a **closed** issue matches, and it was closed
  more than ~8 h ago, so CI's catalog no longer contains it and will **not** auto-match: the next
  failing run gets treated as unknown and `check_ci.py` would file a **duplicate**. Before treating
  it as *the* tracking issue, run two checks:
  - **Is it actually the same failure?** A title/`Test name:` match is not proof. Read the issue
    and confirm the **failure mode** matches — same error/exception text, same assertion or stack
    site, same `STID`, same job/config scope. A broader issue (e.g. a generic `Server died`, or one
    covering a whole job) or a different error under the same test name is **not** a match for the
    specific failure you are exploring — treat that as `needs issue`, do **not** reopen the wrong or
    broader issue.
  - **Does it still fail after `closedAt`?** (step 3 already has the history; if not, run a dated
    query bounded by `check_start_time > <closedAt>` on `master` / across PRs):
    - **Still failing after `closedAt`** (and same failure mode) → the close was premature or the
      fix regressed. Recommend **reopening #N** — preferred over a fresh issue, since it avoids a
      duplicate and CI re-matches it once open again. This is the case the "already fixed → retry"
      line below must **not** swallow.
    - **No failures after `closedAt`** and the fix commit post-dates the failing run → genuinely
      **already fixed**; recommend retry/rebase, not reopen.
  Report it as `stale #N` with the recommendation (reopen vs retry vs needs-new) spelled out.

Never label a cell with an asserted fact you did not verify (e.g. "(known)" implying a tracking
issue exists). `generic failure / untracked` requires that you actually ran the search and it
returned no match — it is not a substitute for searching.

Record the matching issue number, its state (open vs closed/`completed`), **`closedAt`** (needed for
the ~8 h window and the stale/reopen check), labels, and any root cause or fix PR mentioned. A
closed `completed` issue whose fix post-dates the failing run points
at "retry, already fixed" — **but only if the same failure has not recurred on `master` since the
issue's `closedAt`** (see the stale/reopen check above); if it has, the fix regressed and the issue
is a reopen candidate, not an "already fixed". If the input was itself an issue (step 0) you already
have it, but still scan for duplicate or related issues and read its comments.

#### 2b. Existing fix + its status

Independently of whether an issue exists, search for a **fix** — a PR that addresses this failure.
Three complementary sources, cheapest first:

- **From the tracking issue:** a fix PR is usually linked from the issue. `closedByPullRequestsReferences`
  names the PR(s) that closed it, and `comments` often mention the fix (`gh issue view --json` does
  **not** support a `timelineItems` field — it errors `Unknown JSON field`):

  ```bash
  .claude/tools/gh-ro.sh issue view <issue-number> --repo ClickHouse/ClickHouse --json number,state,stateReason,closedByPullRequestsReferences,comments
  ```

  This surfaces PRs that **closed** the issue and any fix named in comments — but **not** a PR that
  merely cross-references it (`Related #<issue>`) without closing it. The issue timeline would show
  those, but the investigate profile denies `gh api` (it can POST). So look for them with a
  read-only **PR search by issue number** — scoped to title/body, and treated as **candidates
  only**:

  ```bash
  .claude/tools/gh-ro.sh pr list --repo ClickHouse/ClickHouse --state all --limit 20 \
    --search "<issue-number> in:title,body" \
    --json number,title,state,isDraft,mergedAt,mergeCommit,headRefName,url
  ```

  **A bare number is ambiguous — every hit is a candidate, never a confirmed fix.** GitHub matches
  the number as free text, so this returns PRs that merely *mention* it (e.g. searching `75982`
  returns this docs PR, which only cites `#75982`). Before using any hit in the `Fix` column,
  **verify it genuinely references this issue** — its body has a relationship line naming the issue,
  in **either** ClickHouse's full-URL convention (`Closes:`/`Fixes:`/`Related:`
  `https://github.com/ClickHouse/ClickHouse/issues/<n>` — the form repo PRs are supposed to use) or
  the short `Closes/Fixes/Related #<n>` form, or it plainly addresses the same failing test/symptom.
  Discard bare prose mentions that don't. Never emit `WIP`/`merged` from an unverified number match.

  Do **not** rely on the by-test-name search below to catch cross-references — a fix PR that
  references the issue but never names the test is invisible to it, which would emit a false
  `Fix: none`.

  The by-issue-number `pr list` above already returns the classification fields. But
  `closedByPullRequestsReferences` and comment mentions give only a PR **number/url** — so for any
  fix PR you learned of only as a bare number (and that the search above did not already return),
  fetch the classification fields explicitly before scoring the Fix column:

  ```bash
  .claude/tools/gh-ro.sh pr view <pr> --repo ClickHouse/ClickHouse --json number,title,state,isDraft,mergedAt,mergeCommit,headRefName,url
  ```

- **By test name:** search PRs (open **and** merged) whose title/body names the test or its
  fragment:

  ```bash
  .claude/tools/gh-ro.sh pr list --repo ClickHouse/ClickHouse --state all --limit 20 \
    --search "<distinctive test-name fragment>" \
    --json number,title,state,isDraft,mergedAt,mergeCommit,headRefName,url
  ```

- **By symptom:** for a `REAL`/`UNCERTAIN` failure, once step 5 names the suspect `file:line`,
  search PRs touching that file or the error string the same way.

**Every search here (by issue number, by test name, by symptom) returns candidates only.** GitHub
matches the term as free text, so a PR that merely *mentions* the test/symptom comes back too — e.g.
searching `test_dns_cache` returns this very docs PR because its text names that test. Before a hit
may fill the `Fix` column you must **confirm it actually addresses this failure**: open its diff
(`.claude/tools/gh-ro.sh pr diff <pr>`) and check the change targets the failing test/code, not just
a passing mention. **Ignore the PR under investigation itself** (the `$0` PR) unless its own diff
genuinely fixes the failure. Discard unconfirmed hits — a `none` is correct when nothing verifiably
addresses the failure; never emit `WIP`/`merged` from an unverified name/symptom match.

For each **verified** fix PR, classify its **status** — this is what goes in the report's Fix column:

- **WIP** — open PR. Note draft vs in-review (`isDraft`). Not yet protecting any run.
- **Merged** — `state == MERGED`, with a `mergeCommit`. Then decide **whether it is already in the
  failing run** using the containment rule below (never from `mergedAt` alone), which determines the
  recommendation:
  - **Merged, present in this branch** → the fix was in the failing run yet the test still failed.
    Incomplete or unrelated — do **not** treat as resolved; keep investigating (step 5).
  - **Merged, not in this branch (master PR report)** → the fix landed on master after the report's
    commit → rebase/retry. The common "already fixed on master — rebase and retry" case.
  - **Merged, presence unverified (backport/`REF`/non-master)** → cannot confirm containment; keep
    the failure open, do not call it fixed.
- **None** — no fix PR found.

**Deciding "already in this branch or not".** Compare the fix's merge against the report commit
(`SHA` from step 1). If the fix's merge commit is in the local object store, check ancestry:

```bash
git cat-file -e <mergeCommit> && git merge-base --is-ancestor <mergeCommit> <report-sha> \
  && echo "fix commit IS in the failing run" || echo "fix commit is NOT in the failing run history (or absent locally)"
```

This check is **asymmetric — trust only the positive**. A **negative never concludes on its own**,
on *any* report type: the exact merge commit being absent does not prove the fix *logic* is absent,
because the branch may carry it under a different SHA (a cherry-pick, a copy merged from elsewhere,
or the same patch authored directly in the PR). So:

- **`is-ancestor` true** → the fix is definitely present. If the test still failed, the fix is
  incomplete/unrelated → keep investigating; do **not** report "already fixed".
- **`is-ancestor` false (or the merge commit is absent locally)** → **do not conclude yet, and do
  not conclude from `git log` alone.** Establish what the fix actually changed, then check that
  **content** against the failing branch:

  1. Get the fix's actual change (its hunks) — the diff of the merged PR:

     ```bash
     .claude/tools/gh-ro.sh pr diff <fix-pr> --repo ClickHouse/ClickHouse
     ```

  2. Use `git log` only to *find candidate* commits on the branch — never as the verdict (a
     `--grep` hit can be a coincidental subject; a `-- <file>` hit only proves some commit touched
     that path, not that it carries the fix; and a **miss proves nothing** — the fix may be present
     after conflict resolution, a refactor, or manual transcription):

     ```bash
     git log --oneline <report-sha> --grep "<fix PR title or key phrase>"   # candidates only
     git log --oneline <report-sha> -- <file the fix touched>               # candidates only
     ```

  3. **Verify at the content level** before emitting any verdict: inspect the fix's key hunk in the
     branch's own version of the file at the report commit (and/or a candidate commit), e.g.
     `git show <report-sha>:<path>` and look for the specific guard/line/logic the fix added, or
     `git show <candidate>` to confirm it is the same change.

  Then:
  - **The fix's logic is present in the branch (content confirmed)** → `merged #N — in branch`
    (if the test still failed, it is incomplete → keep digging). Holds regardless of report type.
  - **The fix's logic is confirmed absent from the branch's file content, and the base is `master`**
    → `merged #N — not in this branch → rebase/retry`.
  - **Anything you could not confirm at the content level** — no access to the fix diff, a
    backport/`REF`/non-master base, the commit absent locally, or an ambiguous/refactored match →
    `merged #N — presence unverified`; keep the failure open for step 5. Never emit `in branch` or
    `rebase/retry` from a `git log` subject/path hit-or-miss alone.

Time order is a weak last resort, not a substitute: a fix `mergedAt` **after** the run's
commit/`check_start_time` cannot be in the run, but "before" does **not** prove presence (the branch
may predate or not contain it). Use it only to rule *out*, and say you relied on it.

### 3. Flaky-vs-real: query master history

This is the cheap, decisive triage — run it **before** downloading any artifacts (step 4),
since a `FLAKY` verdict usually makes the heavy download unnecessary.

Run **one** batch query against `play.clickhouse.com` for all failed test names. The `checks`
table is publicly readable via the `play` user. The query below always computes the same fixed
7/14/30/90-day buckets; `$1` (default `14`) does **not** change the SQL — it selects **which
precomputed bucket is the gate** you read for the verdict (`fail_<$1>d`). If `$1` is not one of
7/14/30/90, round to the nearest bucket (or add that column).

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
  - The PR *adds* this test (`.claude/tools/gh-ro.sh pr diff` shows the test file as new) → new test, judge on its
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
`.claude/tools/gh-ro.sh pr diff <PR>` for the suspect change). For a large category of failures —
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
- the PR diff for cross-referencing — run `.claude/tools/gh-ro.sh pr diff <PR>` and pass its output (or have the
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
- **Issue** (from step 2a) ∈ {`tracked #N`, `needs issue`, `generic failure / untracked` (generic
  bucket or anonymized error class — searched, no `testing` issue matched, and not worth filing),
  `fix instead` (REAL — don't mask), `stale #N` (same-failure issue closed >~8 h ago → CI no longer
  auto-matches; give the recommendation: reopen if it still fails after `closedAt`, retry if truly
  fixed, or needs-new if the closed issue is broader/a different failure mode)}. The search runs for
  every failure (generic buckets included — e.g. `Server died` is often `tracked`); never write a
  tracking claim like "(known)", and never use `untracked` as a stand-in for not searching.
- **Fix** (from step 2b) ∈ {`none`, `WIP #N` (open; note draft), `merged #N — in branch` (fix was
  present yet test still failed → keep digging), `merged #N — rebase/retry` (landed after the run),
  `merged #N — presence unverified` (ancestry inconclusive on a backport/release/`REF` report and no
  cherry-pick confirmed — do **not** call it fixed; keep the failure open for step 5)}.
- For `REAL`/`UNCERTAIN`, give the `file:line` evidence and the suspect PR change.
- For `FLAKY`, state the master frequency and the tracking flaky-test issue from step 2 (number,
  state, and any root cause / fix PR its comments revealed).
- End with a one-line recommendation per test that combines verdict, issue, and fix, e.g.:
  - "retry — flaky on master, tracked by #N";
  - "retry — already fixed on master by #N (merged after this run), rebase";
  - "flaky on master, **no tracking issue — create one**";
  - "real regression in `<file>`, see `<commit>` — fix the PR, do not file a tracking issue";
  - "merged fix #N is already in this branch but the test still failed — needs manual look";
  - "fix #N merged but presence on this backport/`REF` report is unverified — do not assume fixed;
    check for a cherry-pick, keep investigating".

Include the original report URL (and PR link) so the human can confirm. Do not take any action.
