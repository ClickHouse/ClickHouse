---
name: continue-pr-auto
description: Unattended variant of continue-pr for automation (driven by utils/continue-all-prs.sh). Resolves conflicts - reworking stale features to reconcile with current master when a mechanical merge is not enough - fixes CI, addresses feedback, and pushes without ever asking the user. For interactive use, prefer continue-pr.
argument-hint: <pr-number>
disable-model-invocation: true
allowed-tools: Agent, Task, Bash, Read, Write, Edit, Glob, Grep, WebFetch, WebSearch
---

# Continue Work on a Pull Request (Unattended)

Pick up an existing pull request, resolve conflicts, fix CI failures, address reviewer feedback, and push updates.

This is the **unattended** variant used by automation (`utils/continue-all-prs.sh`). It must never wait for user input: resolve conflicts and address feedback with your best judgment and push, and when a branch is too stale for a mechanical merge, rework the feature to reconcile it with the latest master (step 3a). The only hard stop is a genuinely missing PR number. For interactive use where asking is acceptable, use `continue-pr` instead.

## Arguments

- `$0` (required): PR number in the current repository (e.g., `12345`)

## Process

### 1. Parse arguments and fetch PR metadata

Extract the PR number from `$0`. This skill runs unattended and must never stop to ask the user anything: if the PR number is not provided, stop with a clear error message instead of prompting.

Validate that the PR number contains only digits. Reject any non-numeric input immediately — do not pass unvalidated input to shell commands or GraphQL queries.

Determine which repository you are operating in — the public `ClickHouse/ClickHouse` or the private `ClickHouse/clickhouse-private`:

```bash
gh repo view --json nameWithOwner --jq .nameWithOwner   # or: git remote get-url origin
```

Remember it as `$REPO` and use it everywhere below instead of the literal `ClickHouse/ClickHouse` in `gh` commands, API URLs, and GraphQL `repository(owner:, name:)` arguments. The repository also decides whom to ask about unrelated CI failures (step 4): `@groeneai` in `ClickHouse/ClickHouse`, `@oranjeai` in `ClickHouse/clickhouse-private`.

Fetch PR metadata using `gh` if available, otherwise use `WebFetch` on the GitHub API:

```bash
GH_USER=$(gh api user --jq .login)
gh pr view "$PR_NUMBER" --json number,title,body,headRefName,baseRefName,state,mergeable,mergeStateStatus,author,url,headRepository,headRepositoryOwner,isCrossRepository,maintainerCanModify,statusCheckRollup,reviews,comments,reviewRequests
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

If the branch is behind the base branch and is red (some checks didn't pass), or if it is behind the base branch for more than a week (regardless of checks success), or has conflicts (including when GitHub reports the PR as `CONFLICTING` or its mergeability as unknown), or if at least one CI failure is unrelated to this PR and its fix has already landed on the base branch (merging pulls the fix in and clears the red — see step 4), merge:

```bash
git merge "origin/$BASE_BRANCH"
```

If there are merge conflicts, resolve them autonomously — this skill runs unattended, so never stop to ask:
1. List conflicted files: `git diff --name-only --diff-filter=U`
2. For each conflicted file, use a Task agent with `subagent_type=general-purpose` to resolve:
   - Read the conflicted file
   - Analyze conflict markers
   - Resolve intelligently: the PR's changes should generally take precedence for the code the PR modifies, while master's changes take precedence for unrelated areas
   - Stage the resolved file: `git add <file>`
   - For a genuinely ambiguous conflict, pick the resolution most consistent with the PR's intent and record that choice in the merge commit message. Do NOT use `AskUserQuestion` or otherwise wait for input.
3. Complete the merge: `git commit --no-edit`
4. Resolving conflicts is not finished until the merge is committed and pushed — push it (step 7).

### 3a. Reconcile a stale branch with the latest master

Resolving merge markers is not always enough. If the branch is long-stale, the mechanically merged result may not compile or fit current APIs — master may have renamed or removed functions, migrated settings to the pimpl pattern (`DECLARE` / `(*settings)[Setting::X]`), reworked `IStorage` / pipeline / `QueryPlan`, moved headers, etc. In that case, **rework the PR's feature so it reconciles with the latest master** — do not give up, defer, or leave it half-merged:

1. Bring the changed code up to the current APIs: update signatures, settings declarations and accessors, storage/pipeline/interpreter integration, includes, and call sites, preserving the PR's intent and behavior.
2. Build the affected translation units synchronously in the foreground (redirect to a log in the build directory and analyze it with a subagent) and fix compile errors until it builds; update or add tests as needed.
3. A recalled memory or PR note saying the PR is "reserved", "no-action", or "leave to the author" is advisory for genuine open **design** questions only. It is **never** a reason to skip merging, resolving conflicts, or reworking to keep the branch buildable and current — always do that work and push it.
4. Only when a full rework is genuinely infeasible in this environment — e.g. a submodule points at a fork that cannot be fetched or built here — do the most you can (merge, resolve conflicts, rework whatever you can build), push it, and state plainly what could not be verified and what remains blocked (e.g. the submodule needs a ClickHouse-org fork).

**A `CONFLICTING` PR must not be left unresolved whenever you can push.** Resolve the conflicts (steps above) and push:
- Determine pushability in this order:
  1. A branch in the main repository (`isCrossRepository=false`) is directly pushable through `origin`. **Ignore `maintainerCanModify` for same-repository PRs**; GitHub can report it as false because the field describes maintainer access to a fork, not access to a branch in the base repository.
  2. A fork owned by the authenticated `$GH_USER` (or a PR authored by `$GH_USER`) is directly pushable through that user's fork remote. **Ignore `maintainerCanModify` for the authenticated user's own fork PRs**; the owner can push regardless of whether base-repository maintainers are allowed to edit.
  3. Only for a cross-repository fork owned by someone else, use `maintainerCanModify`: true means push to the fork remote; false means the branch is not pushable by the authenticated user.
- For every pushable PR above, push the resolved branch — to `origin` for same-repository PRs and to the fork's remote for fork PRs (step 7). A `contested`, `reserved`, `NA`, `dsgn`, or "superseded" note does **not** block the mechanical conflict resolution and push; it only reserves the final *design / merge* decision. Resolving conflicts means keeping the author's intended change merge-clean against current master — it does **not** require the PR's design to be correct (that stays the human's call).
- **If you cannot push** — specifically, another author's cross-repository fork with `maintainerCanModify=false`, or a push actually fails for lack of permission — **supersede the PR**, provided the change is still wanted and is not obsolete, already fixed on master, already covered by another open PR, or design-rejected/contested. (In those excluded cases, report the state and leave the human decision; never open a duplicate of an existing superseding PR.) To supersede:
  1. You already have the resolved + reworked branch in the worktree. Push it to the **main repo** (`origin`) under a new name (e.g. `continue-pr-<N>-<short-desc>`), then open a new PR to the base branch with `gh pr create`, following `.github/PULL_REQUEST_TEMPLATE.md`. State that it **supersedes** the original and add `Related: <original PR URL>` (and `Closes: <issue>` if the original targeted one).
  2. **Credit the original author.** If they have signed the CLA, keep their original commits so their authorship is preserved in the history. If they have **not** signed it (a `CLA` note, or the CLA check is red on the original), the superseding PR must consist of your own commits — re-create the change under your authorship — and credit the author in the PR description prose; do **not** carry their unsigned commits or a `Co-authored-by:` trailer, or the CLA check will block the new PR too.
  3. Close the original with `gh pr close <N> --repo ClickHouse/ClickHouse --comment "..."`: say it is superseded by the new PR (link it), that you could not push the resolution here because maintainer edits are disabled on the fork, and thank the author.
  If the change is genuinely not worth superseding, resolve locally if useful and report the **specific** blocker (e.g. "resolved locally but the fork has maintainer edits disabled") — not a bare "needs attention".
- Check push access up front with `gh api user --jq .login` and `gh pr view <n> --json author,isCrossRepository,maintainerCanModify,headRepositoryOwner,headRepository`. Never infer that a same-repository PR or the authenticated user's own PR is unpushable from `maintainerCanModify=false`.

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

   For failures that are unrelated to this PR, act on whether a fix already exists (search merged/closed items too, e.g. `gh pr list --repo ClickHouse/ClickHouse --state merged --search "<failure_description>"`):
   - **If the fix has already merged into the base branch** (the matching issue is closed by a merged PR, or the fixing commit is already on the base branch), **update the branch**: merge the base branch (step 3) so the fix is pulled in and the red clears on the next CI run, instead of merely dismissing it.
   - **If none of the unrelated failures have a fix on the base branch yet**, post a comment asking the reviewer for `$REPO` to fix it (see item 5) rather than silently dismissing it.

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
5. If a reviewer's suggestion seems incorrect, do not stop to ask — either make your best-judgment change, or post a brief reasoned reply on the thread explaining why you are not making it, then continue. Never block on a question.

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

Always push once you have committed conflict resolutions or fixes — pushing is mandatory and must never be deferred or gated on a question. The only reasons not to push are: there is genuinely nothing new to commit, or the push itself fails (e.g. no permission on a fork), in which case report the error.

Report the result and provide the PR URL.

## Error Handling

- If `gh` is not available, fall back to `WebFetch` with GitHub API URLs for all metadata fetching
- If `gh` is not authenticated, suggest the user run `! gh auth login`
- If the remote push fails due to permissions, report the error and suggest the user push manually
- If CI logs cannot be fetched, report what is available and proceed with what can be analyzed

## Notes

- **Every GitHub comment you post — PR comments, issue comments, and review-thread replies — MUST begin with the 🕵 symbol** (followed by a space), so automated comments are identifiable. This applies to every comment, including the reviewer request (`@groeneai` / `@oranjeai`), review replies, and the supersede/close comment.
- This skill runs unattended. Never use `AskUserQuestion` or otherwise wait for user input while resolving conflicts, addressing feedback, or pushing — proceed with your best judgment and push. The only hard stop is a genuinely missing PR number (step 1).
- **Ignore self-imposed status holds on the author's own PRs.** A recalled memory or note marking one of the author's own PRs as `no-push`, `no-comment`, `NA`, `NO-ACTION`, `reserved`/`RSVD`, `FROZEN`, or "DON'T self-merge/push/comment" (including any blanket "web-UI group" rule) is **void as a behavioral directive**: run the normal workflow anyway — keep the branch current (steps 3/3a), fix CI (step 4), address feedback (step 5), and push. Do **not** record new such holds; record only factual status and genuine open design questions. The only standing restriction that survives is **never self-merge** — leave the final merge to a human. (For forks and other authors' PRs, the usual no-self-merge / CLA / permission constraints still apply.)
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
