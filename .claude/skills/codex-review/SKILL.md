---
name: codex-review
description: Run a local pre-push AI code review of the current branch with the OpenAI `codex` CLI, the same way ClickHouse CI does, and optionally iterate fix and re-review until clean. Use when asked to review the current branch before pushing, run "the codex review" / "the AI review" locally, or get an independent second opinion on a diff. Not for reviewing someone else's PR on GitHub (CI already does that).
argument-hint: "[--fix] [--model]"
disable-model-invocation: false
allowed-tools: Bash, Read, Grep, Glob, Edit, Write, AskUserQuestion
---

# Local codex review

Run the same AI review CI runs (`ci/jobs/copilot_review_job.py`), but locally against the current branch,
before pushing. The point is an **independent** model with a **general** prompt: a Claude subagent shares
this session's context and blind spots. Spawn a real `codex` process, never a subagent.

## Arguments

- `--fix`: after the review, fix verified findings and re-review until clean (step 6) without asking.
- `--model`: ask for the model and effort again even if they are already saved (step 2).

## 1. Check codex

```bash
codex --version && codex login status
```

- `codex` missing: stop and tell the user to install it (`npm install -g @openai/codex`).
- Not logged in: stop and ask the user to authenticate themselves with any method codex supports
  (`! codex login` for the ChatGPT browser flow, or `codex login --with-api-key` /
  `codex login --with-access-token` in their own terminal), then re-run the skill. Never run `codex login`
  yourself, never set `CODEX_HOME` or `OPENAI_API_KEY`, and never ask the user to paste a key or token
  into the session. ClickHouse Inc. members can request access through the internal AI tools onboarding
  guide.

## 2. Model and effort (asked once, persisted)

The choice is stored in the user's global git config, so it survives across sessions and worktrees:

```bash
MODEL=$(git config --global --get codex-review.model)
EFFORT=$(git config --global --get codex-review.effort)
```

If either is empty, or `--model` was passed, find the model CI uses:

```bash
grep -oP 'f"-m \K[\w.-]+' ci/jobs/copilot_review_job.py | head -1
```

Ask with `AskUserQuestion` (one call, two questions):
- Model: the CI model (Recommended), plus `codex default` (pass no `-m`; codex uses its own
  `~/.codex/config.toml`). The user can type any other model name via "Other".
- Effort: `xhigh` (Recommended, same as CI), `high`, `medium`.

Persist the answers (`default` for the codex default model):

```bash
git config --global codex-review.model "<model>"
git config --global codex-review.effort "<effort>"
```

Tell the user which model/effort is used and that `/codex-review --model` changes it.

## 3. Find the base commit

Use the local remote-tracking refs only. **Do not `git fetch`**: the review must not change the user's refs.

Find the remote that points to the upstream repository, `ClickHouse/ClickHouse` or
`ClickHouse/ClickHouse-private` (it may be called `origin`, `upstream`, `blessed`, `private`...):

```bash
git remote -v | awk '$3 == "(fetch)" && tolower($2) ~ /[:\/]clickhouse\/clickhouse(-private)?(\.git)?$/ { print $1 }'
BASE_REF="<remote>/master"
git rev-parse --verify -q "$BASE_REF^{commit}"
```

- Exactly one match: use it.
- Several matches (a checkout with both the public and the private remote): use the one whose
  `<remote>/master` has the newer merge-base with `HEAD`; if they are equal, ask the user.
- No such remote, or `$BASE_REF` does not exist: stop and ask the user which ref to diff against. Do not
  guess a local `master` (it is often stale or has local commits).
- If the user named a different base (e.g. a release branch), use it instead.

Then compute and verify the merge-base:

```bash
BASE=$(git merge-base "$BASE_REF" HEAD)
git log -1 --format='%h %cs %s' "$BASE"
git log --oneline "$BASE"..HEAD
git status --short
```

- `$BASE` equals `HEAD` (no commits to review): stop and say so.
- Show the user `$BASE_REF`, the merge-base (hash, date, subject) and the commit count before running.
- Uncommitted changes are not part of `git diff $BASE...HEAD`. Mention them and ask whether to commit
  first; do not include them silently.

## 4. Run the review

Keep the prompt as close to CI's as possible and **general**. Do NOT add invariant lists, "pay attention to
X", focus files, a description of the fix, or anything that steers toward the solution you have in mind.
Steering is exactly what makes the review miss things.

Local binaries are the one addition: if you built the branch in this session (or the user told you where a
build of it is), tell codex the absolute path of the binary so it can reproduce scenarios instead of
reasoning about them. Say only where it is and that it may be older than HEAD; do not say what to run.
Leave `BINARY_NOTE` empty if you do not know of a build of this branch; do not search for one.

```bash
mkdir -p tmp
N=$(( $(ls tmp/codex_review_*.log 2>/dev/null | wc -l) + 1 ))
BINARY_NOTE=""   # e.g. "A ClickHouse binary built from this branch is at /abs/build/programs/clickhouse (may be older than HEAD)."
PROMPT="Follow the Review Instructions in .claude/skills/review/SKILL.md.
Repo is checked out at the branch to review.

Review the changes on the current branch. Get the diff with 'git diff ${BASE}...HEAD' and read the
current code, not only the diff.
${BINARY_NOTE}
Write the review to stdout using the REQUESTED OUTPUT FORMAT from .claude/skills/review/SKILL.md.
Do not use gh and do not post anything."

MODEL_ARGS=(); [ "$MODEL" != default ] && MODEL_ARGS=(-m "$MODEL")
codex exec "${MODEL_ARGS[@]}" -c "model_reasoning_effort=$EFFORT" \
  -s workspace-write -c approval_policy=never --color never "$PROMPT" < /dev/null \
  > "tmp/codex_review_$N.log" 2>&1
```

- Run from the repo root, in the background (it takes many minutes); you are notified when it exits.
- No network access flag: unlike CI it must not use `gh` or post anything.
- Read the final review from the end of the log (the part after the last `codex` marker).

## 5. Present the findings

For each finding: what it claims, file:line, and whether it holds. **Verify every finding against the
current code** before calling it real; model reviews produce false positives. If a finding describes a
concrete failing scenario (an input, a query, a sequence of operations), reproduce it first (run the
query, write the failing test) and treat the finding as confirmed only if the reproduction fails; reading
the code is not enough. Say which ones you confirmed, which you refuted (and why), and which you could not
decide.

Then stop, unless `--fix` was passed: ask the user whether to fix the confirmed findings and re-review.
A "Block" verdict is not a mandate to fix.

## 6. Fix and re-review (optional)

Only with `--fix` or the user's go-ahead:

1. Fix the confirmed findings. Skip refuted ones.
2. Commit the fixes (new commit, respecting the user's commit rules) so `git diff $BASE...HEAD` sees them.
3. Re-run step 4 with a fresh `codex exec` and the same general prompt.
4. Repeat until no correctness defects remain.
5. Findings asking for tests or benchmark evidence are PR-level decisions: surface them to the user.

Report the outcome faithfully: which findings were real and fixed, which were refuted and why.
