# How to check what changes were made

## 1. See all modified files (uncommitted)

```bash
cd /home/ubuntu/ClickHouse
git status
```

## 2. See only keeper-bench changes (you are not responsible for these)

```bash
# Summary: which files, how many lines
git diff --stat -- programs/keeper-bench/

# Full diff for each file (review line by line)
git diff -- programs/keeper-bench/Stats.h
git diff -- programs/keeper-bench/Stats.cpp
git diff -- programs/keeper-bench/Runner.h
git diff -- programs/keeper-bench/Runner.cpp
git diff -- programs/keeper-bench/README.md

# Or all keeper-bench in one go
git diff -- programs/keeper-bench/
```

## 3. See what is in the last commit (vs your uncommitted changes)

The **last commit** (49fa3055d4b) does **not** touch `programs/keeper-bench/` at all. It only touches tests/stress/keeper (parquet, core.yaml, SANITIZATION_SUMMARY.md, .gitignore).

```bash
git show HEAD --stat                    # what files the commit changed
git show HEAD -- programs/keeper-bench/ # empty – no keeper-bench in commit
```

## 4. Compare current state to a clean base (e.g. main)

To see everything that differs from another branch (e.g. main), including the last commit:

```bash
git diff main -- programs/keeper-bench/   # keeper-bench vs main
git diff main -- tests/stress/keeper/     # stress tests vs main
git log main..HEAD --oneline              # commits on this branch
```

## 5. Discard all keeper-bench changes (revert to last commit)

If you want to **remove** the keeper-bench changes and go back to what’s in the repo (no replay version-tracking, no Stats fixes, etc.):

```bash
git checkout HEAD -- programs/keeper-bench/
```

Warning: CHA-01-REPLAY will then fail (no JSON errors/ops, possible assert in Stats when read_requests=0, and non-zero replay errors without version tracking).
