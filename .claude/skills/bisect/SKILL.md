---
name: bisect
description: Bisect a ClickHouse regression using pre-built master binaries from CI. Use when the user wants to find the commit that introduced a bug.
argument-hint: <repro.sql> [good-ref] [bad-ref]
disable-model-invocation: false
allowed-tools: Task, Bash(git:*), Bash(curl:*), Bash(chmod:*), Bash(mkdir:*), Bash(ls:*), Bash(/tmp/*), Bash(cat:*), Read, Write, Glob, Grep, WebFetch
---

# ClickHouse Bisect Skill

Bisect a ClickHouse regression using pre-built release binaries downloaded from CI artifacts. This avoids building ClickHouse from source at each bisect step.

## Arguments

- `$0` (required): Path to a SQL repro file (e.g., `/tmp/repro.sql`)
- `$1` (optional): Good ref — a git tag, branch, or SHA known to be good (default: latest `v*.1.1-*` tag, i.e., the previous release branch point)
- `$2` (optional): Bad ref — a git tag, branch, or SHA known to be bad (default: `HEAD`)

## Pre-built Binary URL Pattern

Master CI builds upload the `clickhouse` binary to:

```
https://clickhouse-builds.s3.amazonaws.com/REFs/master/<sha>/build_amd_release/clickhouse
```

where `<sha>` is the full 40-character commit hash.

Only merge commits on `master` have CI-built binaries. Use `--first-parent` in `git bisect` to stay on the merge-commit chain.

## Bisect Process

### 1. Validate inputs

- Verify the repro SQL file exists and read it
- Resolve good/bad refs to full SHAs
- If no good ref is given, find the latest release tag that predates the bad ref:
  ```bash
  git tag -l 'v*' --sort=-creatordate | head -20
  ```
  Then pick the nearest `v<MAJOR>.<MINOR>.1.1-*` tag (the branch-off point for the previous release)

### 2. Confirm the bug reproduces on the bad ref and does not reproduce on the good ref

Before starting bisect, download binaries for the good and bad refs and test them. This catches cases where:
- The repro doesn't actually trigger on the bad ref (user error)
- The repro also triggers on the good ref (wrong good ref)

For each ref:
```bash
SHA=$(git rev-parse <ref>)
URL="https://clickhouse-builds.s3.amazonaws.com/REFs/master/${SHA}/build_amd_release/clickhouse"
curl -sf -o /tmp/bisect_bins/clickhouse_${SHA} "$URL" && chmod +x /tmp/bisect_bins/clickhouse_${SHA}
```

Run the repro and check for the expected failure pattern. Ask the user what constitutes "bad" if it's ambiguous (LOGICAL_ERROR, segfault, wrong result, etc.).

### 3. Write the bisect test script

Create `/tmp/bisect_test.sh` tailored to the specific failure. The script must:

```bash
#!/bin/bash
set -e

SHA=$(git rev-parse HEAD)
BINARY="/tmp/bisect_bins/clickhouse_${SHA}"

# Download binary if not cached
if [ ! -f "$BINARY" ]; then
    URL="https://clickhouse-builds.s3.amazonaws.com/REFs/master/${SHA}/build_amd_release/clickhouse"
    HTTP_CODE=$(curl -s -o "$BINARY" -w "%{http_code}" "$URL")
    if [ "$HTTP_CODE" != "200" ]; then
        rm -f "$BINARY"
        exit 125  # skip — no binary available for this commit
    fi
    chmod +x "$BINARY"
fi

# Run the repro
OUTPUT=$("$BINARY" local --multiquery < /path/to/repro.sql 2>&1 || true)

# Check for the failure pattern (customize per bug)
if echo "$OUTPUT" | grep -q "<FAILURE_PATTERN>"; then
    exit 1   # bad
else
    exit 0   # good
fi
```

**Exit codes:**
- `0` = good (bug not present)
- `1` = bad (bug present)
- `125` = skip (binary not available — `git bisect` will try adjacent commits)

**Failure pattern examples:**
- LOGICAL_ERROR / exception in debug: `grep -q "LOGICAL_ERROR\|Bad cast from type"`
- Segfault: check exit code 139 or grep for `Segmentation fault`
- Wrong result: compare output against expected value
- Specific error code: `grep -q "Code: 49"` (or whichever error code)

### 4. Run git bisect

```bash
mkdir -p /tmp/bisect_bins
git bisect start --first-parent <bad-ref> <good-ref>
git bisect run /tmp/bisect_test.sh
```

**IMPORTANT:** Always use `--first-parent` so git only visits merge commits on the master branch (which have CI binaries).

Set a generous timeout (up to 10 minutes) since each step downloads ~200 MB.

### 5. Report findings

After bisect completes:

```bash
# The output will say: "<sha> is the first bad commit"
# Save it before resetting
git bisect reset
```

Report to the user:
- The first bad commit SHA and its merge commit message
- The PR number (extract from merge commit message, e.g., "Merge pull request #NNNNN")
- A link to the PR: `https://github.com/ClickHouse/ClickHouse/pull/<number>`
- A brief description of what the PR changed (from `git show --stat`)
- If there were skipped commits (no binary available), list them as possible candidates too

### 6. Handle edge cases

**Binary not available (HTTP 403/404):**
- The script returns exit code 125 (skip). Git bisect will try nearby commits.
- If too many consecutive commits have no binary, bisect may fail. In that case:
  - List the commits in the remaining range
  - Check which ones have binaries available
  - Test them manually

**Bisect ends with ambiguity (skipped commits):**
- Git will report "first bad commit could be any of: ..."
- List those candidates and check the range between last known good and first known bad
- Often the range is small enough (2–5 commits) to identify the culprit by inspection

**The repro needs clickhouse-server (not clickhouse-local):**
- For server-based repros, the test script needs to:
  1. Start the server in background with a temporary data directory
  2. Wait for it to be ready
  3. Run the repro via clickhouse-client
  4. Capture output/exit code
  5. Stop the server
  6. Clean up

Example server test script skeleton:
```bash
#!/bin/bash
set -e

SHA=$(git rev-parse HEAD)
BINARY="/tmp/bisect_bins/clickhouse_${SHA}"

# ... download binary same as above ...

DATADIR=$(mktemp -d /tmp/bisect_data_XXXXXX)
LOGFILE="$DATADIR/server.log"

# Start server
"$BINARY" server --config-file /dev/null \
    -- --path="$DATADIR" \
       --tcp_port=29000 \
       --logger.log="$LOGFILE" \
       --logger.level=warning &
SERVER_PID=$!

# Wait for server to be ready (up to 30s)
for i in $(seq 1 30); do
    if "$BINARY" client --port 29000 -q "SELECT 1" &>/dev/null; then
        break
    fi
    sleep 1
done

# Run the repro
OUTPUT=$("$BINARY" client --port 29000 --multiquery < /path/to/repro.sql 2>&1 || true)

# Stop server
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null
rm -rf "$DATADIR"

# Check failure pattern
if echo "$OUTPUT" | grep -q "<FAILURE_PATTERN>"; then
    exit 1
else
    exit 0
fi
```

## Examples

- `/bisect /tmp/repro.sql` — Bisect using auto-detected good ref (previous release) and HEAD as bad
- `/bisect /tmp/repro.sql v26.1.1.1-new` — Bisect between v26.1 (good) and HEAD (bad)
- `/bisect /tmp/repro.sql v26.1.1.1-new abc123def` — Bisect between specific good and bad refs

## Notes

- Binaries are cached in `/tmp/bisect_bins/` to avoid re-downloading on retries
- Each binary is ~200 MB (self-extracting); first run of a downloaded binary prints "Decompressing the binary..."
- Only first-parent (merge) commits on master have CI binaries — never try to test intermediate branch commits
- Always `git bisect reset` when done (whether successful or not) to restore the working tree
- The repro SQL file should be self-contained: create tables, run the failing query, drop tables
- For `clickhouse local` repros, settings can be passed inline in the SQL: `SET allow_suspicious_low_cardinality_types=1;`
- Clean up `/tmp/bisect_bins/` manually when no longer needed — cached binaries can consume significant disk space
