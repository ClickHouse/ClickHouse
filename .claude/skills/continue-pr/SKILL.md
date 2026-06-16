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

Fetch PR metadata using `gh` if available, otherwise use `WebFetch` on the GitHub API:

```bash
gh pr view "$PR_NUMBER" --json number,title,body,headRefName,baseRefName,state,mergeable,mergeStateStatus,author,url,headRepository,headRepositoryOwner,statusCheckRollup,reviews,comments,reviewRequests
```

If `gh` is not available or not authenticated, use WebFetch to get the data. Append `?per_page=100` and follow the `Link` header for pagination (the `rel="next"` URL) to fetch all pages:
- `https://api.github.com/repos/ClickHouse/ClickHouse/pulls/$PR_NUMBER`
- `https://api.github.com/repos/ClickHouse/ClickHouse/pulls/$PR_NUMBER/reviews?per_page=100`
- `https://api.github.com/repos/ClickHouse/ClickHouse/pulls/$PR_NUMBER/comments?per_page=100`

Report the PR title, author, branch, and current state to the user.

### 2. Check out the PR branch locally

Determine whether the PR branch is in the main repository or in the author's fork.

**If the branch is in the main repository (`ClickHouse/ClickHouse`):**
```bash
git fetch origin "$HEAD_BRANCH"
git checkout -b "$HEAD_BRANCH" "origin/$HEAD_BRANCH" 2>/dev/null || git checkout "$HEAD_BRANCH"
git pull origin "$HEAD_BRANCH"
```

**If the branch is in the author's fork:**

Derive the fork clone URL from the PR metadata (`headRepository.url` or `headRepository.owner.login` + `headRepository.name`) rather than hardcoding the repository name (forks can be renamed). Use a `pr-` prefixed remote name to avoid colliding with existing remotes like `origin`:

```bash
REMOTE_NAME="pr-$AUTHOR_LOGIN"
FORK_URL="https://github.com/$FORK_OWNER/$FORK_REPO.git"  # from headRepository in PR metadata
git remote add "$REMOTE_NAME" "$FORK_URL" 2>/dev/null || git remote set-url "$REMOTE_NAME" "$FORK_URL"
git fetch "$REMOTE_NAME" "$HEAD_BRANCH"
git checkout -b "$HEAD_BRANCH" "$REMOTE_NAME/$HEAD_BRANCH" 2>/dev/null || git checkout "$HEAD_BRANCH"
git pull "$REMOTE_NAME" "$HEAD_BRANCH"
```

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

5. If you are confident that the failure is unrelated to the changes, post a comment, asking @groeneai to investigate the failure:
   @groeneai, investigate the failure: <link> and provide a fix in a separate PR. If the fix is already in progress, link it here. 

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
git push origin "$HEAD_BRANCH"
```

**If the branch is in the author's fork:**
```bash
git push "$REMOTE_NAME" "$HEAD_BRANCH"
```

Report the result and provide the PR URL.

## Error Handling

- If `gh` is not available, fall back to `WebFetch` with GitHub API URLs for all metadata fetching
- If `gh` is not authenticated, suggest the user run `! gh auth login`
- If the remote push fails due to permissions, report the error and suggest the user push manually
- If CI logs cannot be fetched, report what is available and proceed with what can be analyzed

## Notes

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
