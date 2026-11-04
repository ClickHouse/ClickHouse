---
name: fix-sync
description: Fix the "CH Inc sync" job in a pull request by resolving conflicts in the corresponding clickhouse-private sync PR.
argument-hint: <pr-number-or-url>
disable-model-invocation: false
allowed-tools: Task, Bash(gh:*), Bash(cd:*), Bash(git:*), Bash(ls:*), Bash(pwd:*), Bash(mktemp:*), Read, Grep, Glob, AskUserQuestion
---

# Fix CH Inc Sync Skill

Fix the "CH Inc sync" CI job for a ClickHouse pull request by resolving merge conflicts in the corresponding `clickhouse-private` sync PR.

## Arguments

- `$0` (required): PR number or full GitHub URL of the public ClickHouse PR (e.g., `96005` or `https://github.com/ClickHouse/ClickHouse/pull/96005`)

## Overview

When a PR is opened in `ClickHouse/ClickHouse`, a sync PR is automatically created in `ClickHouse/clickhouse-private` on a branch named `sync-upstream/pr/<PR_NUMBER>`. If this sync PR has merge conflicts, the "CH Inc sync" check stays pending. This skill resolves those conflicts.

## Process

### 1. Parse the PR number

- Extract the PR number from `$ARGUMENTS`
- If a full URL is provided (e.g., `https://github.com/ClickHouse/ClickHouse/pull/96005`), extract the number from the URL
- If no argument is provided, use `AskUserQuestion` to ask for the PR number

### 2. Find the sync PR

- Search for the corresponding sync PR in the private repository:
  ```bash
  gh pr list --repo ClickHouse/clickhouse-private --head sync-upstream/pr/<PR_NUMBER> --json number,url,state,mergeable,mergeStateStatus,headRefName
  ```
- If no sync PR is found, report this to the user and stop
- If the sync PR is not conflicting (mergeable is not `CONFLICTING`), report that no action is needed and stop
- Report the sync PR number and URL to the user

### 3. Locate the private repository

- Look for the `clickhouse-private` repository in common locations relative to the current working directory:
  - `../ClickHouse_private`
  - `../clickhouse-private`
  - Check if the directory exists and contains a git repository with `ClickHouse/clickhouse-private` as a remote
- If not found, use `AskUserQuestion` to ask the user for the path
- Store the path for use in subsequent steps

### 4. Fetch and switch to the sync branch

In the private repository directory:

```bash
cd <private_repo_path> && git fetch origin && git fetch origin sync-upstream/pr/<PR_NUMBER>
```

Then check out the sync branch:
```bash
cd <private_repo_path> && git checkout sync-upstream/pr/<PR_NUMBER>
```

If the branch has local changes, ask the user before proceeding.

### 5. Merge master and resolve conflicts

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

### 6. Update submodules (if needed)

```bash
cd <private_repo_path> && git submodule update --init --recursive
```

If submodule update fails, report the error but continue.

### 7. Build verification (optional)

Use `AskUserQuestion` to ask the user:
- "Do you want to build ClickHouse in the private repository to verify the merge?"
  - Option 1: "Yes, build" - Run ninja in the private repo build directory
  - Option 2: "No, skip build" - Skip building and proceed to push

If the user chooses to build, use the build skill or run ninja directly.

### 8. Push the resolved branch

```bash
cd <private_repo_path> && git push origin sync-upstream/pr/<PR_NUMBER>
```

### 9. Verify the sync PR

After pushing:
```bash
gh pr view <SYNC_PR_NUMBER> --repo ClickHouse/clickhouse-private --json mergeable,mergeStateStatus
```

Report the result:
- If `mergeable` is `MERGEABLE`: "Sync PR is now mergeable. The CH Inc sync check should pass shortly."
- If still conflicting: "Sync PR still has conflicts. Additional investigation may be needed."

Provide the sync PR URL for the user to check.

## Examples

- `/fix-sync 96005` - Fix sync for PR #96005
- `/fix-sync https://github.com/ClickHouse/ClickHouse/pull/96005` - Fix sync using full URL

## Notes

- The sync branch name follows the pattern: `sync-upstream/pr/<PR_NUMBER>`
- The private repository is typically located one directory level up from the public repository
- Most conflicts are straightforward and involve the upstream changes taking precedence
- Always fetch before switching branches to ensure you have the latest state
- Do not use rebase or amend - add new commits instead (per project conventions)
- After pushing, it may take a few minutes for the GitHub check to update
