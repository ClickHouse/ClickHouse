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
- **Worktrees** (`.git` is a file) are deleted as a whole if no file in them (excluding `build*`, `contrib`, `.git`) is newer than the threshold, and they have no uncommitted changes or local commits that would be lost (including inside submodules).
- **Build dirs** (`build*`) inside kept checkouts are deleted if no file inside is newer than the threshold.
- Anything whose age cannot be established (e.g. unreadable subdirectories) is kept.
- Never delete directories in use by a process, containing mount points (e.g. `/dev` mounted into a chroot), or containing block device files. `delete.sh` checks this right before deleting each entry and skips it.
- Do not touch loose files in the root, or directories that are not git checkouts.

## Process

Run from the root of the ClickHouse repository. Use `tmp/clean-worktrees` for state files. The disk can be slow with a cold cache: every step can take tens of minutes. Run long steps with `run_in_background: true` and the maximum timeout. When waiting, poll with `pgrep -f "[s]can.sh"`-style patterns (the bracket keeps the pattern from matching its own command line). Never use `pkill -f <pattern>` where the pattern appears in your own command: it kills your own shell.

### 1. Scan

```bash
.claude/skills/clean-old-worktrees-global/scan.sh tmp/clean-worktrees "$HOME" '1 day ago'
```

This produces `checkouts.txt`, `wt_old_status.txt`, `build_old.txt`, and `skipped.txt` (see the header of the script). Fields are separated by tabs, because directory names can contain spaces. Always split them with tabs (`cut -f`, `awk -F '\t'`), never with whitespace.

### 2. Review safety

Checkouts with age `UNKNOWN` and entries in `skipped.txt` are kept; mention them in the report.

Inspect every line of `wt_old_status.txt` that is not `dirty=0 unpushed=0 submodule_commits=0`:

- `unpushed>0` on a named branch: safe. The branch lives in the main repo and survives worktree removal. Confirm afterwards that the branch still exists (check all main repos, the worktree may belong to a different repo than its name suggests).
- `unpushed>0` with `branch=HEAD` (detached): the commits would become unreachable. Ask the user before deleting.
- `dirty>0`: look at `git -C <dir> status --porcelain`. Test leftovers (parquet files, outputs) are junk. Real source modifications, including modified content or new commits in submodules: ask the user. Submodule working trees and their git directories are deleted with the worktree; the branch in the main repo does not preserve them.
- `submodule_commits>0`: unpushed commits change submodule pointers. Check that every referenced submodule commit is on a remote branch of the submodule (`git -C <dir>/<submodule> branch -r --contains <commit>`); otherwise ask the user.
- `ERROR` in any field: the state could not be established. If it is `fatal: not a git repository` (the worktree is orphaned, its metadata already pruned), there is no recoverable git state; treat it as an ordinary old directory. Otherwise keep the worktree.

Build the deletion list (paths relative to root) from old worktrees (minus anything excluded above) plus old build dirs:

```bash
cut -f1 tmp/clean-worktrees/wt_old_status.txt > tmp/clean-worktrees/delete_list.txt
cat tmp/clean-worktrees/build_old.txt >> tmp/clean-worktrees/delete_list.txt
```

Remove the excluded worktrees from the list (match whole lines, e.g. `grep -vxF`).

### 3. Delete

```bash
.claude/skills/clean-old-worktrees-global/delete.sh tmp/clean-worktrees/delete_list.txt "$HOME"
```

Use the same root as for the scan. Run it in the background with the maximum timeout (deleting 100+ worktrees took more than an hour). If it is stopped by the time limit, rerun the same command: it is idempotent and also prunes the worktree metadata.

Right before deleting each entry, the script rejects main repositories, symlinks, and paths that are not a direct child of the root or a `build*` directory inside one. It skips (`SKIPPED (<reason>): <dir>`) directories in use by a process (working directory, root, executable, open or mapped file), containing a mount point, or containing a block device file, and deletes with `rm --one-file-system`. If a skipped directory is used only by your own leftover processes (e.g. a timed-out `du`), stop them by PID and rerun; otherwise keep it.

Without passwordless sudo, processes of other users (e.g. in Docker containers) cannot be inspected, and the script refuses to run. In that case check `docker ps` and `ps` for processes using the candidates, then rerun with `ALLOW_UNINSPECTED=1`.

### 4. Report

Report: number of deleted worktrees per main repo, number of deleted build dirs (with the notable ones), disk usage before and after (`df -h`), which unpushed branches were preserved and where, anything skipped and why (including `SKIPPED` lines of `delete.sh`), and what was kept (main repos, recent checkouts, non-worktree repos).
