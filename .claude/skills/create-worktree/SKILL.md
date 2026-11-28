---
name: create-worktree
description: Create a ClickHouse git worktree whose submodules share storage with the main checkout, add submodules to a worktree that was created without them, or update the submodules of a worktree after a merge moved their pins. Use when the user wants a new worktree for ClickHouse development, or when the submodules of a worktree must follow new pins.
argument-hint: <branch-name>
disable-model-invocation: false
allowed-tools: Bash(git:*), Bash(ls:*), Bash(pwd:*), Bash(${CLAUDE_SKILL_DIR}/worktree.sh *), AskUserQuestion
---

# Create ClickHouse Worktree Skill

Create a git worktree for ClickHouse development. The script `worktree.sh` next to this file does the work. The submodules of the worktree share storage with the main checkout:

- macOS (APFS): the submodule git data and the submodule files are clones (`cp -c`) of the main checkout. A worktree costs about 1.7 GiB (1.3 GiB of superproject files, 0.4 GiB for the submodules) instead of 11-27 GiB, and the submodule setup takes about 3 minutes.
- Linux: the submodule object files are hardlinks, and the submodule files are checked out (about 10 GiB, because ext4 has no clones).

The script never runs `git submodule update`. By default it uses the main checkout as it is, and it uses the network only for submodule commits that no local git data has. Only on request (`--fresh-master`) it fetches `master` of the superproject and updates the main checkout.

## Rules

- **Never run `git submodule update --init`, or any `git submodule update`, in a worktree.** In a worktree, it clones each submodule from GitHub into a private copy of the git data (15-17 GiB in total) and checks out the files again. Use `worktree.sh update-submodules` instead.
- **Remove a worktree with `git -C <MAIN_REPO> worktree remove --force <WORKTREE_PATH>`.** It also deletes `<MAIN_REPO>/.git/worktrees/<name>`, which holds the submodule git data. If the directory is already gone, run `git -C <MAIN_REPO> worktree prune`, which deletes that data. Deleting only the worktree directory leaves the data on disk.
- Remove a worktree when its PR is merged. Each worktree has its own submodule files.

## Arguments

- `$0` (required): Branch name. If the branch exists, the worktree checks it out. If it does not exist, a new branch is created.

## Process

### 1. Determine the main repo

Find the main checkout (`MAIN_REPO`), also when the current directory is a worktree:

```bash
dirname "$(git rev-parse --path-format=absolute --git-common-dir)"
```

If this fails, the current directory is not a git checkout: report an error and stop.

### 2. Validate inputs

- Make sure that `$0` (branch name) is given. If not, use `AskUserQuestion` to ask the user for a branch name.
- Compute `SAFE_BRANCH`: the branch name with each `/` replaced by `-`. For example, branch `release/25.12` gives `SAFE_BRANCH=release-25.12`. This prevents nested directories.
- Use `AskUserQuestion` to ask the user for the **worktree destination path**. Suggest `<MAIN_REPO>/../<MAIN_REPO_NAME>-<SAFE_BRANCH>` as the default: a sibling of the main checkout, named after the repo and the branch (for example `../ClickHouse-my-feature`). The user can enter a different path.

Let `WORKTREE_PATH` be the chosen path.

### 3. Choose the start point

- The branch exists locally: no start point.
- The branch exists only on a remote: fetch it, and use `origin/<branch-name>` as the start point.
  ```bash
  git -C <MAIN_REPO> fetch --no-recurse-submodules origin <branch-name>
  ```
- New branch: give no start point. The script then starts the branch from the current `master` of the main checkout, as it is, without network.
- Pass `--fresh-master` only when the user asks for the latest `master`. The script then fetches the upstream of `master`, updates the main checkout when that is safe, and starts a new branch from the fresh upstream.
- A different base (for example `public/master` in a private checkout, or a release branch): fetch it, and give it as the start point.
  ```bash
  git -C <MAIN_REPO> fetch --no-recurse-submodules public master    # start point: public/master
  ```

### 4. Decide whether to set up submodules

Submodules are needed only to build or to run tests in the worktree.

- **Skip them** (`--no-submodules`) when the work is text only: docs, comments, changelog entries, `.md` files, typo fixes that need no compile.
- **Set them up** when the work involves `ninja` or tests in this worktree.

If you are not sure, use `AskUserQuestion`: "Will you build or run tests in this worktree? (No: skip the submodule setup, much faster)". Submodules can be added later with `update-submodules`.

### 5. Run the script

Run the script next to this file, with its output in a log file. With submodules, it takes several minutes. Run it in the background.

```bash
${CLAUDE_SKILL_DIR}/worktree.sh create [--no-submodules] [--fresh-master] <MAIN_REPO> <WORKTREE_PATH> <branch-name> [<start-point>] \
    > <MAIN_REPO>/tmp/create-worktree-<SAFE_BRANCH>.log 2>&1
```

If the script prints `FAILED`, report the error to the user. Do not work around it.

### 6. Report results

Report to the user:
- Main repo: `<MAIN_REPO>`
- Worktree path: `<WORKTREE_PATH>`
- Branch: `<branch-name>` (new or existing)
- With `--fresh-master`: what the script reported about the main checkout (updated, or why not)
- Submodules: "set up, sharing storage with the main checkout" or "skipped (text-only task)"
- Suggest: `cd <WORKTREE_PATH>`

## Later: update or add submodules

Run this when a merge or a checkout moved submodule pins in the worktree (for example a merge of master), when a worktree created with `--no-submodules` now needs a build, or when an earlier setup was interrupted:

```bash
${CLAUDE_SKILL_DIR}/worktree.sh update-submodules [--fresh-master] <WORKTREE_PATH>
```

- Add `--fresh-master` only when the user asks to update the main checkout too, for example after merging a fresh `master` into the worktree.

- It keeps the existing submodule git data of the worktree (`<MAIN_REPO>/.git/worktrees/<name>/modules`), with its local branches, stashes and commits, and copies only the missing git data from the main checkout. Never delete that directory to "repair" a worktree: it can hold the only copy of local submodule work.
- It reads the pins from the index, so it also works during a merge, after the conflicts in the submodules are resolved.
- It does not use `-f` for submodules that are already checked out, and it never replaces ignored local files (`--no-overwrite-ignore`): when local changes or ignored local files are in the way, it fails for that submodule, and the user decides.
- When an earlier setup was interrupted, it finishes the first checkout of each submodule that did not complete it. A marker file in the git data of the submodule shows this; the final check fails while a marker is left.
- If a submodule directory has files that this script did not set up (a checkout that uses other git data, or files without a checkout), it stops before any change and lists those directories.

## What the script does

1. **Prepares the main checkout and its git data.**
   - By default it uses the main checkout as it is: no fetch, and no change to its files. With `--fresh-master`, it fetches the upstream of `master`; a missing upstream or a failed fetch fails the run. If the main checkout is on a clean `master`, it then fast-forwards `master` to that upstream. A submodule of the main checkout moves to its new pin only when nobody works on it: it is detached at the previous pin and has no changes in tracked files. Otherwise the files stay as they are, and the script says why: other sessions can use the main checkout. The fast-forward and the submodule moves never replace an ignored local file whose path the new commit tracks (`--no-overwrite-ignore`); in that case the main checkout or the submodule stays as it is, with a note, and the worktree setup continues.
   - It makes the main git data (`<MAIN_REPO>/.git/modules/*`) contain every submodule commit that the worktree needs and does not have yet. It uses the network only for missing commits, from the URL in the `.gitmodules` of the worktree (a branch can move a submodule to a fork). It keeps each commit fetched by SHA under `refs/pins/<sha>`, so that `git gc` does not remove it, and checks that the commit and its history arrived; a shallow source (`contrib/update-submodules.sh` uses `--depth=1`) is supported. It accepts only plain repository URLs (`https://`, `ssh://`, `git@host:path`, `file://`, an absolute path) and safe submodule names and paths, because the `.gitmodules` of a branch can come from someone else. A submodule that is new in the worktree is cloned into the main git data, so that the next worktree can share it.
2. **Copies the missing submodule git data** from `<MAIN_REPO>/.git/modules` to `<MAIN_REPO>/.git/worktrees/<name>/modules`, only for the submodules of the worktree: clones on macOS; on Linux, hardlinks for the object files and real copies for all other files, so that a fetch in one worktree (`FETCH_HEAD`, reflogs, index) does not change the others.
3. **Sets up the submodule files.** On macOS, it clones the files of each submodule from the main checkout, and then checks out only the files that differ from the recorded commit. On Linux, it checks out the recorded commit. The heaviest submodules start first.
4. **Checks the result:** every submodule must be at the commit that the superproject records. It also lists the submodules that have modified files. On macOS, `contrib/liburing` and `contrib/sysroot` always show modified files, because they have file names that differ only in case.

## Notes

- The script ignores `submodule.recurse` from the user configuration: it handles each submodule itself.
- The main checkout must have its submodules initialized once (`git submodule update --init` in the main checkout, not in a worktree).
- Build directories are not shared. Set up CMake and the build in the new worktree separately.
- `SUBMODULE_CHECKOUT_WORKERS` sets the parallel checkout workers in one submodule: by default 2 on macOS, where the clones leave little to check out, and `min(8, CPUs)` on Linux. `SUBMODULE_JOBS` (default `CPUs / SUBMODULE_CHECKOUT_WORKERS`) sets how many submodules are processed at the same time.

## Examples

- `/create-worktree my-feature`: create a worktree with a new branch `my-feature`.
- `/create-worktree fix/issue-12345`: create a worktree. The branch name can contain slashes; they become dashes in the default directory name.
