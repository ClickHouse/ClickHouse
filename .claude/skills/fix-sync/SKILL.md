---
name: fix-sync
description: Fix the "CH Inc sync" job in a pull request. Diagnoses the failure as one of three types — conflicts found, build failed, or tests failed — and handles each accordingly.
argument-hint: <pr-number-or-url>
disable-model-invocation: false
allowed-tools: Task, Bash(gh:*), Bash(cd:*), Bash(git:*), Bash(ls:*), Bash(pwd:*), Bash(mktemp:*), Read, Grep, Glob, AskUserQuestion
---

# Fix CH Inc Sync Skill

Fix the "CH Inc sync" CI job for a ClickHouse pull request. The job runs the corresponding `clickhouse-private` sync PR through merge and private CI, so it can fail in three distinct ways. This skill first **diagnoses** which type of failure occurred, then applies the matching fix.

## The three failure types

The "CH Inc sync" commit status on the public PR reports its failure as one of three descriptions (set by the private CI in `clickhouse-private`: `ci/jobs/private_sync_pr.py` for conflicts, `ci/jobs/scripts/workflow_hooks/private_sync_complete_check.py` for build/test failures):

1. **`conflicts found`** — the sync PR cannot be merged with `master` of `clickhouse-private` (`mergeable == CONFLICTING`). Resolve the merge conflicts and push.
2. **`build failed`** — the sync PR merges cleanly, but a build job in the private CI fails. Usually the upstream change needs adaptation in private-only code that references changed symbols. Fix the build and push.
3. **`tests failed`** — the sync PR merges and builds, but test jobs fail. The failure is either flaky/infra-related (rerun the failed jobs) or a genuine regression (investigate).

The `pending` description `testing` means CI is still running — wait rather than act.

## Arguments

- `$0` (required): PR number or full GitHub URL of the public ClickHouse PR (e.g., `96005` or `https://github.com/ClickHouse/ClickHouse/pull/96005`)

## Overview

When a PR is opened in `ClickHouse/ClickHouse`, a sync PR is automatically created in `ClickHouse/clickhouse-private` on a branch named `sync-upstream/pr/<PR_NUMBER>`. The "CH Inc sync" check on the public PR reflects the state of that private sync PR: it stays pending/failing if the sync PR has conflicts, fails to build, or has failing tests.

## Process

### 1. Parse the PR number

- Extract the PR number from `$ARGUMENTS`
- If a full URL is provided (e.g., `https://github.com/ClickHouse/ClickHouse/pull/96005`), extract the number from the URL
- If no argument is provided, use `AskUserQuestion` to ask for the PR number

### 2. Find the sync PR

- Search for the corresponding sync PR in the private repository:
  ```bash
  gh pr list --repo ClickHouse/clickhouse-private --head sync-upstream/pr/<PR_NUMBER> --json number,url,state,mergeable,mergeStateStatus,headRefName,headRefOid
  ```
- If no sync PR is found, report this to the user and stop
- Report the sync PR number and URL to the user

### 3. Diagnose the failure type

This is the key step. Determine which of the three failure types applies before doing any work.

1. **Read the "CH Inc sync" commit status description on the public PR** — it directly states the failure type:
   ```bash
   gh api repos/ClickHouse/ClickHouse/commits/<HEAD_SHA>/statuses --jq '.[] | select(.context == "CH Inc sync") | {state, description}'
   ```
   (Get `<HEAD_SHA>` from `gh pr view <PR_NUMBER> --repo ClickHouse/ClickHouse --json headRefOid`.)
   Map the description to a handler:
   - `conflicts found` → **Conflicts found** → step 4A.
   - `build failed` → **Build failed** → step 4B.
   - `tests failed` → **Tests failed** → step 4C.
   - `testing` (state `pending`) → CI is still running. Report this and stop — wait rather than act.
   - `completed` (state `success`) → no action needed, stop.

2. **If the status description is missing or ambiguous**, fall back to inspecting the sync PR directly:
   - If `mergeable` is `CONFLICTING` (or `mergeStateStatus` is `DIRTY`) → **Conflicts found** → step 4A.
   - Otherwise inspect the private CI checks: `gh pr checks <SYNC_PR_NUMBER> --repo ClickHouse/clickhouse-private`. A failing job whose name starts with `Build` → **Build failed** (step 4B); only test jobs failing → **Tests failed** (step 4C).

Report the diagnosed failure type to the user before proceeding.

### 4. Locate the private repository (needed for 4A and 4B)

- Look for the `clickhouse-private` repository in common locations relative to the current working directory:
  - `../ClickHouse_private`
  - `../clickhouse-private`
  - Check if the directory exists and contains a git repository with `ClickHouse/clickhouse-private` as a remote
- If not found, use `AskUserQuestion` to ask the user for the path
- Store the path for use in subsequent steps

---

### 4A. Handle "Conflicts found"

#### Fetch and switch to the sync branch

In the private repository directory:

```bash
cd <private_repo_path> && git fetch origin && git fetch origin sync-upstream/pr/<PR_NUMBER>
```

Then check out the sync branch:
```bash
cd <private_repo_path> && git checkout sync-upstream/pr/<PR_NUMBER>
```

If the branch has local changes, ask the user before proceeding.

#### Merge master and resolve conflicts

```bash
cd <private_repo_path> && git merge origin/master
```

This will likely produce conflicts. Handle them:

1. **List conflicted files:**
   ```bash
   cd <private_repo_path> && git diff --name-only --diff-filter=U
   ```

2. **For each conflicted file**, use a Task agent with `subagent_type=general-purpose` to resolve:
   - Read the conflicted file content
   - Analyze the conflict markers (`<<<<<<<`, `=======`, `>>>>>>>`)
   - Determine the correct resolution:
     - For most sync conflicts, the upstream (public repo) changes should take precedence
     - For files that exist only in the private repo, preserve them
     - For CI/workflow files, be careful to preserve private-repo-specific configurations
   - Apply the resolution using Edit tool
   - Stage the resolved file: `git add <file>`

   **IMPORTANT:** If conflicts are complex or ambiguous, show the conflicts to the user and ask how to resolve them using `AskUserQuestion`.

3. **After resolving all conflicts**, complete the merge:
   ```bash
   cd <private_repo_path> && git commit --no-edit
   ```

#### Update submodules (if needed)

```bash
cd <private_repo_path> && git submodule update --init --recursive
```

If submodule update fails, report the error but continue.

Then go to step 5 (push and verify).

---

### 4B. Handle "Build failed"

A clean merge that fails to build almost always means the upstream change renamed, moved, or changed the signature of something that **private-only** code depends on, or the merge needs a code adaptation that conflict resolution did not surface.

1. **Fetch the build log** to find the exact error. Identify the failed build run and download its log:
   ```bash
   gh run list --repo ClickHouse/clickhouse-private --branch sync-upstream/pr/<PR_NUMBER> --json databaseId,name,conclusion,headSha
   ```
   Use the public CI log tool when a report URL is available (see `.claude/tools/fetch_ci_report.js`), or inspect the failed job log via `gh run view --log-failed`.

2. **Locate the build error** (compile error, linker error, missing symbol). Use a Task agent with `subagent_type=general-purpose` to analyze the log and return only the relevant error excerpt and the file/symbol involved.

3. **Reproduce and fix in the private repo.** Check out the sync branch (fetch + merge `origin/master` if not already merged), then fix the offending private code to match the upstream change. Prefer adapting the private code to the new upstream API rather than reverting the upstream change.

4. **Optionally rebuild to verify** (see step 5's build verification).

Then go to step 5 (push and verify).

---

### 4C. Handle "Tests failed"

Merge and build are fine, but tests fail. First decide whether the failure is **flaky/infra** or **genuine**.

1. **Fetch the failing test details:**
   ```bash
   gh pr checks <SYNC_PR_NUMBER> --repo ClickHouse/clickhouse-private
   gh run list --repo ClickHouse/clickhouse-private --branch sync-upstream/pr/<PR_NUMBER> --json databaseId,name,conclusion
   ```

2. **Classify as flaky/infra if** the failures are infrastructure errors unrelated to the PR's changes, e.g. `Cannot start clickhouse-server`, `Timeout`, network/disk errors, or failing tests that touch areas the PR does not modify. (See the `pr-89842-sync-rerun` note: a PR touching only `src/IO/*` had `Cannot start clickhouse-server` / `Timeout` failures — clearly unrelated.)
   - **Action:** rerun only the failed jobs, do **not** run a merge:
     ```bash
     gh run rerun <RUN_ID> --repo ClickHouse/clickhouse-private --failed
     ```
   - Report that a rerun was triggered and stop.

3. **Otherwise, treat as a genuine regression.** Use a Task agent to fetch and summarize the failing test logs, then report the failing tests and the likely cause to the user. Do not push speculative fixes without confirmation — use `AskUserQuestion` to decide next steps.

This path normally does not push to the sync branch (unless a genuine fix is made), so skip step 5 unless a code change was committed.

---

### 5. Push the resolved branch (for 4A / 4B, and 4C only if a fix was committed)

#### Build verification (optional)

Use `AskUserQuestion` to ask the user:
- "Do you want to build ClickHouse in the private repository to verify the merge/fix?"
  - Option 1: "Yes, build" - Run ninja in the private repo build directory (redirect output to a build log; use a subagent to summarize)
  - Option 2: "No, skip build" - Skip building and proceed to push

#### Push

```bash
cd <private_repo_path> && git push origin sync-upstream/pr/<PR_NUMBER>
```

### 6. Verify the sync PR

After pushing (or rerunning):
```bash
gh pr view <SYNC_PR_NUMBER> --repo ClickHouse/clickhouse-private --json mergeable,mergeStateStatus
gh pr checks <SYNC_PR_NUMBER> --repo ClickHouse/clickhouse-private
```

Report the result per failure type:
- **Conflicts found:** if `mergeable` is now `MERGEABLE`: "Sync PR is now mergeable. The CH Inc sync check should pass shortly." If still conflicting: "Sync PR still has conflicts. Additional investigation may be needed."
- **Build failed:** report whether the build job now passes (it may take a while for CI to start).
- **Tests failed:** report whether the rerun is in progress or the fix was pushed.

Provide the sync PR URL for the user to check.

## Examples

- `/fix-sync 96005` - Diagnose and fix sync for PR #96005
- `/fix-sync https://github.com/ClickHouse/ClickHouse/pull/96005` - Fix sync using full URL

## Notes

- Always diagnose the failure type (step 3) before acting. Running a merge against a sync PR that is already `MERGEABLE` does nothing useful — for build/test failures the fix is different (see the `pr-89842-sync-rerun` note: rerun failed jobs rather than `/fix-sync` when no merge conflict exists).
- The sync branch name follows the pattern: `sync-upstream/pr/<PR_NUMBER>`
- The private repository is typically located one directory level up from the public repository
- Most conflicts are straightforward and involve the upstream changes taking precedence
- Always fetch before switching branches to ensure you have the latest state
- Do not use rebase or amend - add new commits instead (per project conventions)
- After pushing, it may take a few minutes for the GitHub check to update
