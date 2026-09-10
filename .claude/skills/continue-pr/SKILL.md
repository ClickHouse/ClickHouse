---
name: continue-pr
description: Continue work on an existing PR - resolve conflicts, fix CI failures, address reviewer feedback, and push updates. Use when the user wants to pick up and advance a pull request.
argument-hint: <pr-number>
disable-model-invocation: false
allowed-tools: Agent, Task, Bash, Read, Write, Edit, Glob, Grep, WebFetch, WebSearch, AskUserQuestion
---

# Continue Work on a Pull Request

Pick up an existing pull request, resolve conflicts, fix CI failures, address reviewer feedback, and push updates.

## Arguments

- `$0` (required): PR number in the current repository (e.g., `12345`)

## Process

### 1. Parse arguments and fetch PR metadata

Extract the PR number from `$0`. If not provided, use `AskUserQuestion` to ask for it.

Validate that the PR number contains only digits. Reject any non-numeric input immediately — do not pass unvalidated input to shell commands or GraphQL queries.

Determine which repository you are operating in — the public `ClickHouse/ClickHouse` or the private `ClickHouse/clickhouse-private`:

```bash
gh repo view --json nameWithOwner --jq .nameWithOwner   # or: git remote get-url origin
```

Remember it as `$REPO` and use it everywhere below instead of the literal `ClickHouse/ClickHouse` in `gh` commands, API URLs, and GraphQL `repository(owner:, name:)` arguments. The repository also decides whom to ask about unrelated CI failures (step 4, item 5): `@groeneai` in `ClickHouse/ClickHouse`, `@oranjeai` in `ClickHouse/clickhouse-private`.

Fetch PR metadata using `gh` if available, otherwise use `WebFetch` on the GitHub API:

```bash
gh pr view "$PR_NUMBER" --json number,title,body,headRefName,baseRefName,state,mergeable,mergeStateStatus,author,url,headRepository,headRepositoryOwner,statusCheckRollup,reviews,comments,reviewRequests
```

If `gh` is not available or not authenticated, use WebFetch to get the data. Append `?per_page=100` and follow the `Link` header for pagination (the `rel="next"` URL) to fetch all pages:
- `https://api.github.com/repos/ClickHouse/ClickHouse/pulls/$PR_NUMBER`
- `https://api.github.com/repos/ClickHouse/ClickHouse/pulls/$PR_NUMBER/reviews?per_page=100`
- `https://api.github.com/repos/ClickHouse/ClickHouse/pulls/$PR_NUMBER/comments?per_page=100`

Report the PR title, author, branch, and current state to the user.

### 1a. Verify "already integrated" claims before closing

Treat closing a PR as already integrated or obsolete as a high-confidence decision. Verify both of these independently against freshly fetched refs:

1. **Historical ancestry:** Resolve and print the exact base and PR-head OIDs, then check the candidate commit in this direction only:
   ```bash
   HEAD_REMOTE=origin
   if [ "$IS_CROSS_REPOSITORY" = "true" ]; then
       HEAD_REMOTE="pr-$AUTHOR_LOGIN"
       git remote add "$HEAD_REMOTE" "$FORK_URL" 2>/dev/null || git remote set-url "$HEAD_REMOTE" "$FORK_URL"
   fi
   git fetch origin "$BASE_BRANCH"
   git fetch "$HEAD_REMOTE" "$HEAD_BRANCH"
   git rev-parse "origin/$BASE_BRANCH" "$HEAD_REMOTE/$HEAD_BRANCH"
   git merge-base --is-ancestor "$CANDIDATE_COMMIT" "origin/$BASE_BRANCH"
   ```
   Derive `$IS_CROSS_REPOSITORY`, `$AUTHOR_LOGIN`, and `$FORK_URL` from the PR metadata as in step 2. Exit status `0` means the base contains the commit; any other status means it does not. Never check against `HEAD`, the PR branch, `--all`, or a worktree and describe that result as containment by the base branch. If this check is nonzero, do not claim the commit is in the base and do not close the PR as integrated.
2. **Current effective state:** An ancestor commit may have been reverted or backed out later. Ancestry, `git branch --contains`, `git cherry`, and patch-equivalence results are historical evidence only; none proves that the change remains effective. Inspect all later base-branch commits touching the changed paths, search for explicit and manual reverts/backouts, compare the current base tree with the PR's intended effect, and run the reproducer or regression test against the current base when feasible. If the change was fully or partially reverted, count it as **not integrated**. If the current effect cannot be established confidently, leave the PR open for human review.

Before posting a closure comment, state the fetched base OID and the evidence for both ancestry and the current effective state. Do not close from a remembered ref, a stale local branch, a matching subject, or a commit merely visible somewhere in the repository.

### 2. Check out the PR branch locally

Before checking out the PR, require a clean worktree. This interactive skill
can run in a developer's normal checkout, so it must fail closed rather than
discarding staged, unstaged, or untracked work:

```bash
test -z "$(git status --porcelain)" || {
    echo "The worktree is dirty; use a clean worktree before continuing this PR" >&2
    exit 1
}
```

Determine whether the PR branch is in the main repository or in the author's fork.

**If the branch is in the main repository (`ClickHouse/ClickHouse`):**
```bash
HEAD_REMOTE=origin
git fetch "$HEAD_REMOTE" "$HEAD_BRANCH"
git checkout --detach "$HEAD_REMOTE/$HEAD_BRANCH"
```

**If the branch is in the author's fork:**

Derive the fork clone URL from the PR metadata (`headRepository.url` or `headRepository.owner.login` + `headRepository.name`) rather than hardcoding the repository name (forks can be renamed). Use a `pr-` prefixed remote name to avoid colliding with existing remotes like `origin`:

```bash
REMOTE_NAME="pr-$AUTHOR_LOGIN"
FORK_URL="https://github.com/$FORK_OWNER/$FORK_REPO.git"  # from headRepository in PR metadata
git remote add "$REMOTE_NAME" "$FORK_URL" 2>/dev/null || git remote set-url "$REMOTE_NAME" "$FORK_URL"
HEAD_REMOTE="$REMOTE_NAME"
git fetch "$HEAD_REMOTE" "$HEAD_BRANCH"
git checkout --detach "$HEAD_REMOTE/$HEAD_BRANCH"
```

After the clean checkout, record the immutable baseline; it is the only
authoritative record of the PR surface that existed when the worker began:

```bash
git fetch origin "$BASE_BRANCH"
mkdir -p tmp
PR_BASELINE_DIR="$(pwd)/tmp/continue-pr-${PR_NUMBER}-baseline"
rm -rf "$PR_BASELINE_DIR"
mkdir -p "$PR_BASELINE_DIR"
INITIAL_PR_HEAD=$(git rev-parse "$HEAD_REMOTE/$HEAD_BRANCH")
test "$(git rev-parse HEAD)" = "$INITIAL_PR_HEAD"
INITIAL_BASE_HEAD=$(git rev-parse "origin/$BASE_BRANCH")
printf '%s\n%s\n' "$INITIAL_PR_HEAD" "$INITIAL_BASE_HEAD" > "$PR_BASELINE_DIR/state"
git diff --name-status "origin/$BASE_BRANCH"...HEAD > "$PR_BASELINE_DIR/name-status"
git diff --stat "origin/$BASE_BRANCH"...HEAD > "$PR_BASELINE_DIR/stat"
git diff --binary "origin/$BASE_BRANCH"...HEAD > "$PR_BASELINE_DIR/diff"
```

Do not modify, stage, or delete this artifact during the session. It must remain
available through the push safety gate in step 7. The deterministic path and
`state` file are required because each resumed worker turn starts a fresh shell
process.

### 3. Resolve conflicts with the base branch (if any)

Use the PR's actual base branch from metadata (`baseRefName`) instead of hardcoding `master` — this ensures backport PRs targeting other branches are handled correctly.

Check if the PR has conflicts with the base branch:

```bash
git fetch origin "$BASE_BRANCH"
git merge-base --is-ancestor "origin/$BASE_BRANCH" HEAD || echo "needs merge"
```

If the branch is behind the base branch and is red (some checks didn't pass), or if it is behind the base branch for more than a week (regardless of checks success), or has conflicts, merge:

```bash
git merge "origin/$BASE_BRANCH"
```

If there are merge conflicts:
1. List conflicted files: `git diff --name-only --diff-filter=U`
2. For each conflicted file, use a Task agent with `subagent_type=general-purpose` to resolve:
   - Read the conflicted file
   - Analyze conflict markers
   - Resolve intelligently: the PR's changes should generally take precedence for the code the PR modifies, while master's changes take precedence for unrelated areas
   - Stage the resolved file: `git add <file>`
   - If conflicts are ambiguous, show them to the user using `AskUserQuestion`
3. Complete the merge: `git commit --no-edit`

### 4. Analyze CI status and fix failures

Use the CI analysis tool to fetch reports:

```bash
node .claude/tools/fetch_ci_report.js "https://github.com/ClickHouse/ClickHouse/pull/$PR_NUMBER" --failed --cidb
```

For each CI failure:

1. **Check if it is a known issue:** Search for existing open issues or PRs that address this failure:
   ```bash
   gh issue list --repo ClickHouse/ClickHouse --state open --search "<failure_description>" --limit 5
   gh pr list --repo ClickHouse/ClickHouse --state open --search "<failure_description>" --limit 5
   ```
   Only dismiss a failure as unrelated if there is a concrete open issue or PR that matches. Do NOT dismiss failures without evidence.

2. **Investigate the failure:** Download logs if needed:
   ```bash
   node .claude/tools/fetch_ci_report.js "<report_url>" --failed --download-logs
   ```
   Extract and analyze the relevant logs. Read the failing test files and the code they exercise.

3. **Fix the failure:** Make the necessary code or test changes. Each fix should be a separate commit with a clear message explaining what was wrong and why.

4. If the only failure is "CH Inc sync", fix it using the /fix-sync skill.

5. If you are confident that the failure is unrelated to the changes, post a comment, asking the reviewer for `$REPO` (step 1) — `@groeneai` in `ClickHouse/ClickHouse`, `@oranjeai` in `ClickHouse/clickhouse-private` — to investigate the failure:
   🕵 @<reviewer>, investigate the failure: <link> and provide a fix in a separate PR. If the fix is already in progress, link it here. 

6. **Repeat** until all failures are addressed or confirmed as known issues with links to open issues/PRs.

### 5. Address reviewer feedback

Fetch review comments:

```bash
gh pr view "$PR_NUMBER" --json reviews,comments --jq '.reviews[] | select(.state != "COMMENTED" or .body != "") | {author: .author.login, state: .state, body: .body}'
gh api "repos/ClickHouse/ClickHouse/pulls/$PR_NUMBER/comments" --paginate --jq '.[] | select(.in_reply_to_id == null or .in_reply_to_id == 0) | {author: .user.login, body: .body, path: .path, line: .line, created_at: .created_at}'
```

Also fetch review comment threads to identify which are resolved and which are not.

**If `gh` is available:**

```bash
# Paginate through all review threads (PRs may have more than 100)
CURSOR=""
while true; do
  AFTER_CLAUSE=""
  if [ -n "$CURSOR" ]; then
    AFTER_CLAUSE=", after: \"$CURSOR\""
  fi
  RESULT=$(gh api graphql -f query="
  {
    repository(owner: \"ClickHouse\", name: \"ClickHouse\") {
      pullRequest(number: $PR_NUMBER) {
        reviewThreads(first: 100${AFTER_CLAUSE}) {
          pageInfo { hasNextPage endCursor }
          nodes {
            id
            isResolved
            comments(first: 100) {
              pageInfo { hasNextPage endCursor }
              nodes {
                author { login }
                body
                path
                line
              }
            }
          }
        }
      }
    }
  }")
  echo "$RESULT"
  HAS_NEXT=$(echo "$RESULT" | jq -r '.data.repository.pullRequest.reviewThreads.pageInfo.hasNextPage')
  [ "$HAS_NEXT" = "true" ] || break
  CURSOR=$(echo "$RESULT" | jq -r '.data.repository.pullRequest.reviewThreads.pageInfo.endCursor')
done
```

If any thread has `comments.pageInfo.hasNextPage == true`, issue a follow-up GraphQL query using the thread's `id` and the `endCursor` to fetch remaining comments:

```bash
gh api graphql -f query="
{
  node(id: \"<thread_id>\") {
    ... on PullRequestReviewThread {
      comments(first: 100, after: \"<end_cursor>\") {
        pageInfo { hasNextPage endCursor }
        nodes {
          author { login }
          body
          path
          line
        }
      }
    }
  }
}"
```

Repeat until `hasNextPage` is `false`.

**If `gh` is not available (WebFetch fallback):**

The GraphQL API for review threads requires authentication, so unresolved-thread detection is not possible via `WebFetch`. In this case:
1. Fetch all review comments from the REST API: `https://api.github.com/repos/ClickHouse/ClickHouse/pulls/$PR_NUMBER/comments?per_page=100` (follow pagination via `Link` header)
2. Group comments by `pull_request_review_id` and `in_reply_to_id` to reconstruct threads
3. Treat all threads as potentially unresolved (since resolution status is only available via GraphQL)
4. Note in the output that thread resolution status could not be determined without `gh` authentication

Filter out resolved threads before processing — only consider threads where `isResolved == false`. Skip resolved threads entirely to avoid reintroducing already-addressed feedback.

For each unresolved review thread:
1. Read the comment and understand what the reviewer is asking for
2. Read the relevant code context
3. Make the requested change if it is reasonable and correct
4. Commit the change with a message referencing the feedback (e.g., "Address review: <summary of change>")
5. If a reviewer's suggestion seems incorrect or unclear, note it in your output for the user to decide

### 6. Review and evaluate the changes

After all fixes are applied, review the complete diff of the PR:

```bash
git diff "origin/$BASE_BRANCH"...HEAD --stat
git log "origin/$BASE_BRANCH"..HEAD --oneline
```

Evaluate the changes holistically:
- Are the changes correct and complete?
- Are there any remaining issues you notice?
- Are tests adequate?
- Is the PR description still accurate after the changes?

Report your assessment to the user.

### 7. Push the changes

Determine where to push based on step 2:

**If the branch is in the main repository:**
```bash
PUSH_REMOTE=origin
```

**If the branch is in the author's fork:**
```bash
PUSH_REMOTE="$REMOTE_NAME"
```

Before every commit and push, run this safety gate. It is a hard stop:

1. Restore and validate the recorded state before using it; resumed turns do not inherit shell variables:
   ```bash
   PR_BASELINE_DIR="$(pwd)/tmp/continue-pr-${PR_NUMBER}-baseline"
   mapfile -t BASELINE_STATE < "$PR_BASELINE_DIR/state"
   test "${#BASELINE_STATE[@]}" = 2
   INITIAL_PR_HEAD=${BASELINE_STATE[0]}
   INITIAL_BASE_HEAD=${BASELINE_STATE[1]}
   git cat-file -e "$INITIAL_PR_HEAD^{commit}"
   git cat-file -e "$INITIAL_BASE_HEAD^{commit}"
   git merge-base --is-ancestor "$INITIAL_PR_HEAD" HEAD
   ```
   Preserve `INITIAL_PR_HEAD` as an ancestor: only add commits on top of the existing PR history. Never rebase, reset the branch onto another commit, amend published commits, delete the remote branch, use a `+` refspec, or pass `--force`, `--force-with-lease`, or `--no-verify` to `git push`.
2. Stage only explicit paths with `git add <path>`. Never use `git add -A`, `git add .`, or `git commit -a` in this workflow. Before committing, inspect both `git diff --cached --name-status` and `git diff --cached --stat`. Every staged path must be explained by the requested fix or by a specific conflict resolution. Unstage unexpected paths and do not commit when the scope is unclear.
3. Before pushing, fetch the remote PR branch again and require its current tip to be an ancestor of local `HEAD`:
   ```bash
   git fetch "$PUSH_REMOTE" "$HEAD_BRANCH"
   REMOTE_HEAD=$(git rev-parse "$PUSH_REMOTE/$HEAD_BRANCH")
   git merge-base --is-ancestor "$REMOTE_HEAD" HEAD
   ```
   If the check is nonzero, merge the remote branch normally or stop and report the lineage problem. Never replace its history.
4. Inspect the complete proposed PR diff with `git diff --name-status "origin/$BASE_BRANCH"...HEAD` and `git diff --stat "origin/$BASE_BRANCH"...HEAD`, then compare both against the immutable checkout baseline:
   ```bash
   diff -u "$PR_BASELINE_DIR/name-status" <(git diff --name-status "origin/$BASE_BRANCH"...HEAD)
   diff -u "$PR_BASELINE_DIR/stat" <(git diff --stat "origin/$BASE_BRANCH"...HEAD)
   ```
   Account explicitly for every added path and every removed path, including changes that disappeared because they were incorporated through a deliberately merged base branch. A sudden broad expansion, contraction, unrelated subtree change, mass deletion, or single-parent fix commit containing base-branch churn indicates a wrong checkout, stale-tree snapshot, contaminated worktree, or lost history; do not push it. If scope cannot be proven from the PR intent and work performed in this session, stop and report the exact diff anomaly.

After the gate succeeds, push with:

```bash
git push "$PUSH_REMOTE" HEAD:"$HEAD_BRANCH"
```

Report the result and provide the PR URL.

## Error Handling

- If `gh` is not available, fall back to `WebFetch` with GitHub API URLs for all metadata fetching
- If `gh` is not authenticated, suggest the user run `! gh auth login`
- If the remote push fails due to permissions, report the error and suggest the user push manually
- If CI logs cannot be fetched, report what is available and proceed with what can be analyzed

## Notes

- **Every GitHub comment you post — PR comments, issue comments, and review-thread replies — MUST begin with the 🕵 symbol** (followed by a space), so automated comments are identifiable.
- Do not use rebase or amend - always add new commits (per project conventions)
- Do not push to the master branch
- Each fix should be a separate, well-described commit
- When writing commit messages, wrap literal names from ClickHouse SQL, classes, functions, or log messages in inline code blocks
- Use Allman-style braces in any C++ code changes
- When building ClickHouse after changes, redirect output to a log file in the build directory and use a subagent to analyze it
- When running tests, redirect output to a log file and use a subagent to analyze it

## 8. Fix unrelated CI failures

After completing all work on the current PR (steps 1–7), review the CI failures that were identified as unrelated in step 4 — i.e., failures proven not caused by this PR's changes and not already being fixed by other open PRs.

For each such unrelated failure:

1. **Switch to master** and create a new branch:
   ```bash
   git checkout master
   git pull origin master
   git checkout -b fix/<descriptive-name>
   ```

2. **Investigate and fix** the failure: download logs, read the failing test and exercised code, and make the fix. Each fix goes on its own branch with its own PR.

3. **Push and open a PR:**
   ```bash
   git push -u origin fix/<descriptive-name>
   ```
   Create a PR using `gh pr create` following the project's PR template (`.github/PULL_REQUEST_TEMPLATE.md`). Link to the open issue if one exists. Use the "CI Fix or improvement" changelog category.

4. **Repeat** for each unrelated failure, one PR per fix.

After all fixes are submitted, switch back to the original PR branch and report the list of new PRs created.
