---
name: bisect
description: Bisect a ClickHouse regression using pre-built master binaries from CI. Use when the user wants to find the commit that introduced a bug.
argument-hint: <repro.sql> [good-ref] [bad-ref]
disable-model-invocation: false
allowed-tools: Task, Bash(git:*), Bash(curl:*), Bash(chmod:*), Bash(mkdir:*), Bash(ls:*), Bash(/tmp/*), Bash(cat:*), Bash(utils/auto-bisect/*), Read, Write, Glob, Grep, WebFetch, LSP
---

# ClickHouse Bisect Skill

Bisect a ClickHouse regression using `utils/auto-bisect/bisect.sh`, which downloads pre-built CI binaries and manages the bisect loop automatically.

## Arguments

- `$0` (required): Path to a SQL repro file (e.g., `/tmp/repro.sql`)
- `$1` (optional): Good ref — a git tag, branch, or SHA known to be good (default: latest `v*.1.1-*` tag)
- `$2` (optional): Bad ref — a git tag, branch, or SHA known to be bad (default: `HEAD`)

## How the auto-bisect script works

`utils/auto-bisect/bisect.sh` orchestrates the full bisect:
1. For each commit, `helpers/download.sh` fetches the release binary from CI S3
2. `env/<option>.sh` installs configs and starts a ClickHouse server (configs go to `utils/auto-bisect/data/etc/`, never to `/etc/`)
3. Your test script is called with `$COMMIT_SHA`, `$CH_PATH` (binary), and `$GIT_WORK_TREE` in the environment
4. Exit code from your test determines good/bad/skip

The working tree is **never checked out** — `BISECT_HEAD` is written for each step so git history is untouched.

## Test script contract

The test script receives these environment variables:
- `$COMMIT_SHA` — full SHA of the commit being tested
- `$CH_PATH` — path to the downloaded binary (e.g., `utils/auto-bisect/data/clickhouse`)
- `$GIT_WORK_TREE` — path to the ClickHouse repository

Exit codes:
- `0` — good (bug not present)
- `1` — bad (bug present)
- `125` — skip (handled upstream by download.sh, not typically needed in the test itself)

## Process

### 1. Validate inputs

- Read and understand the repro SQL
- Resolve good/bad refs to full SHAs: `git rev-parse <ref>`
- If no good ref given, find the previous release branch point:
  ```bash
  git tag -l 'v*' --sort=-creatordate | grep '\.1\.1-' | head -5
  ```
  Pick the nearest `v<MAJOR>.<MINOR>.1.1-*` tag before the bad ref.

### 2. Write the test script

Create `/tmp/bisect_test.sh`. Use `$CH_PATH` for the binary — do **not** download it yourself, the framework handles that.

**For `clickhouse local` repros (no server needed — use `--env nothing`):**

```bash
#!/bin/bash
set -e

OUTPUT=$("$CH_PATH" local --multiquery 2>&1 <<'SQL'
-- paste repro SQL here
SQL
)

if echo "$OUTPUT" | grep -q "FAILURE_PATTERN"; then
    exit 1  # bad
else
    exit 0  # good
fi
```

**For server-based repros (use `--env single`, the default):**

```bash
#!/bin/bash
# Server is already running when this script is called.
# Use clickhouse-client on the default port (9000).
set -e

OUTPUT=$("$CH_PATH" client --multiquery 2>&1 <<'SQL'
-- paste repro SQL here
SQL
)

if echo "$OUTPUT" | grep -q "FAILURE_PATTERN"; then
    exit 1
else
    exit 0
fi
```

Failure pattern examples:
- Exception: `grep -q "LOGICAL_ERROR\|Code: [0-9]"`
- Wrong result: compare `OUTPUT` to expected string
- Segfault: `[ "${PIPESTATUS[0]}" = "139" ]`

### 3. Confirm the bug range

Before running the full bisect, verify the repro:
```bash
GOOD_SHA=$(git rev-parse <good-ref>)
BAD_SHA=$(git rev-parse <bad-ref>)
```

Optionally do a quick walker sanity check over just the two endpoints:
```bash
CH_PATH=/usr/local/bin/clickhouse /tmp/bisect_test.sh  # should exit 0 on good
```

### 4. Run bisect

```bash
cd utils/auto-bisect
./bisect.sh \
  --good <good-sha> \
  --bad  <bad-sha> \
  --path /path/to/ClickHouse \
  --test /tmp/bisect_test.sh \
  --env  nothing          # or: single (default), replicateddb, sharedcatalog
```

Use `--env nothing` when the test uses `clickhouse local` (no server).
Use `--env single` (default) when the test needs a running server.

**Walker mode** — walk every commit linearly instead of bisecting (useful when you want to see the progression or the range is small):
```bash
./bisect.sh --good <sha> --bad <sha> --path ... --test ... --walker
# Or provide explicit commits:
./bisect.sh --path ... --test ... --walker --walker-commits "sha1 sha2 sha3"
# Or limit to N evenly-spaced steps:
./bisect.sh --good <sha> --bad <sha> --path ... --test ... --walker --walker-steps 10
```

Set a generous timeout (up to 10 min) — each step downloads ~200 MB.

### 5. Report findings

After bisect completes, the output contains `<sha> is the first bad commit`.

```bash
git show --stat <bad-sha>
```

Report:
- First bad commit SHA and message
- PR number from message (e.g., "Merge pull request #NNNNN") → link to `https://github.com/ClickHouse/ClickHouse/pull/<N>`
- Brief description from `git show --stat`
- Any skipped commits (no CI binary) that fall in the range

## Notes

- Binaries are downloaded to `utils/auto-bisect/data/clickhouse` and overwritten each step (not cached between steps — use walker if you want to re-test)
- Only first-parent merge commits on master have CI binaries; `bisect.sh` uses `--first-parent` automatically
- Configs are installed to `utils/auto-bisect/data/etc/clickhouse-server/` — the host's `/etc/clickhouse-*` is never touched
- The working tree is never modified during bisect or walker runs; `BISECT_HEAD` is used instead of `git checkout`
- For private CI builds, set `CH_CI_USER` / `CH_CI_PASSWORD` and pass `--private`
