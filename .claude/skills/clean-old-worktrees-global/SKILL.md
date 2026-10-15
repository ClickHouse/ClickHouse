---
name: clean-old-worktrees-global
description: Find and delete unused git worktrees and build directories under the home directory (or another root), treating everything not modified within a given age (default one day) as unused. Use when the user asks to clean up old worktrees, stale build directories, or free disk space across all checkouts.
argument-hint: "[age, default '1 day ago'] [root, default $HOME]"
disable-model-invocation: false
---

# Clean Old Worktrees (Global)

Delete unused git worktrees and stale build directories across all checkouts directly under a root directory.

## Arguments

- Age threshold: anything without modifications newer than this is unused. Default: `1 day ago` (any `find -newermt` expression).
- Root: default `$HOME`.

## Rules

- **Main repos** (`.git` is a directory) are never deleted. Only their old `build*` dirs are.
- **Worktrees** (`.git` is a file) are deleted as a whole if no file in them (excluding `build*`, `contrib`, `.git`) is newer than the threshold.
- **Build dirs** (`build*`) inside kept checkouts are deleted if no file inside is newer than the threshold.
- Do not touch loose files in the root, or directories that are not git checkouts.

## Process

Run from the root of the ClickHouse repository. Use `tmp/clean-worktrees` for state files. The disk can be slow with a cold cache: every step can take tens of minutes. Run long steps with `run_in_background: true` and the maximum timeout. When waiting, poll with `pgrep -f "[s]can.sh"`-style patterns (the bracket keeps the pattern from matching its own command line). Never use `pkill -f <pattern>` where the pattern appears in your own command: it kills your own shell.

### 1. Scan

```bash
.claude/skills/clean-old-worktrees-global/scan.sh tmp/clean-worktrees "$HOME" '1 day ago'
```

This produces `checkouts.txt`, `wt_old_status.txt`, and `build_old.txt` (see the header of the script).

### 2. Review safety

Inspect every line of `wt_old_status.txt` that is not `dirty=0 unpushed=0`:

- `unpushed>0` on a named branch: safe. The branch lives in the main repo and survives worktree removal. Confirm afterwards that the branch still exists (check all main repos, the worktree may belong to a different repo than its name suggests).
- `unpushed>0` with `branch=HEAD` (detached): the commits would become unreachable. Ask the user before deleting.
- `dirty>0`: look at `git -C <dir> status --porcelain --ignore-submodules=all`. Test leftovers (parquet files, outputs) are junk. Real source modifications: ask the user.
- `fatal: not a git repository` errors mean the worktree is orphaned (metadata already pruned). It has no recoverable git state; treat it as an ordinary old directory.

Build the deletion list (paths relative to root) from old worktrees (minus anything excluded above) plus old build dirs:

```bash
cut -d' ' -f1 tmp/clean-worktrees/wt_old_status.txt > tmp/clean-worktrees/delete_list.txt
cat tmp/clean-worktrees/build_old.txt >> tmp/clean-worktrees/delete_list.txt
```

Check that no process has its working directory inside a candidate:

```bash
for p in /proc/[0-9]*; do readlink $p/cwd 2>/dev/null; done | grep -F -f <(sed "s#^#$HOME/#" tmp/clean-worktrees/delete_list.txt)
```

Kill only your own leftover processes (e.g. a timed-out `du`). If a user process is there, skip that directory.

### 3. Delete

```bash
.claude/skills/clean-old-worktrees-global/delete.sh tmp/clean-worktrees/delete_list.txt "$HOME"
```

Run it in the background with the maximum timeout (deleting 100+ worktrees took more than an hour). If it is stopped by the time limit, rerun the same command: it is idempotent and also prunes the worktree metadata.

### 4. Report

Report: number of deleted worktrees per main repo, number of deleted build dirs (with the notable ones), disk usage before and after (`df -h`), which unpushed branches were preserved and where, anything skipped and why, and what was kept (main repos, recent checkouts, non-worktree repos).
