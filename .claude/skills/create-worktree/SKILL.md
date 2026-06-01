---
name: create-worktree
description: Create a ClickHouse git worktree with submodules hardlinked from the main repo. Use when the user wants to create a new worktree for ClickHouse development.
argument-hint: <branch-name>
disable-model-invocation: false
allowed-tools: Bash(git:*), Bash(cp:*), Bash(ln:*), Bash(ls:*), Bash(rm:*), Bash(mkdir:*), Bash(find:*), Bash(pwd:*), AskUserQuestion
---

# Create ClickHouse Worktree Skill

Create a new git worktree for ClickHouse development. When the worktree will be built or tested, submodules are hardlinked from the main repo (independent copies, no network, no extra disk for git objects). For text-only tasks (docs, comments, typo fixes), submodule setup is skipped — it's the slow part of the workflow and is pure overhead when nothing will be compiled.

## Arguments

- `$0` (required): Branch name. If the branch already exists, the worktree will check it out. If it doesn't exist, a new branch will be created from the current HEAD of the main repo.

## Process

### 1. Determine the source repo

- Detect the source repo (`MAIN_REPO`) by running `git rev-parse --show-toplevel` from the current working directory.
- Verify it is a git repository. If not, report an error and stop.

### 2. Validate inputs

- Ensure `$0` (branch name) is provided. If not, use `AskUserQuestion` to ask the user for a branch name.
- Compute `SAFE_BRANCH` by replacing all `/` characters in the branch name with `-`. For example, branch `release/25.12` → `SAFE_BRANCH=release-25.12`. This avoids creating nested directories from slashes in branch names.
- Use `AskUserQuestion` to ask the user for the **worktree destination path**. Suggest `<MAIN_REPO>/../<MAIN_REPO_NAME>-<SAFE_BRANCH>` as the default — this places the worktree as a sibling of the main repo directory, named after both the repo and the branch (e.g. `../ClickHouse-my-feature` or `../ClickHouse-release-25.12`). The user may enter a different path if preferred.

Let `WORKTREE_PATH` be the chosen path (resolved to an absolute path).

### 3. Determine worktree path and branch state

- Worktree path: `<WORKTREE_PATH>` (from step 2)
- Check if the worktree directory already exists. If it does, report to the user and stop.
- Check if the branch already exists:
  ```bash
  git -C <MAIN_REPO> branch --list <branch-name>
  git -C <MAIN_REPO> branch --list -r "origin/<branch-name>"
  ```

### 4. Create the worktree

Create the worktree with `--no-checkout` so git only writes the metadata, then
populate the working tree in a separate parallel checkout. A plain `git worktree
add` checks out all ~40k top-level files single-threaded; `--no-checkout` followed
by a parallel `git checkout` (using `checkout.workers`) is noticeably faster.

**If branch exists locally:**
```bash
git -C <MAIN_REPO> worktree add --no-checkout <WORKTREE_PATH> <branch-name>
```

**If branch exists on remote only:**
```bash
git -C <MAIN_REPO> worktree add --no-checkout <WORKTREE_PATH> -b <branch-name> origin/<branch-name>
```

**If branch does not exist (create new):**
```bash
git -C <MAIN_REPO> worktree add --no-checkout -b <branch-name> <WORKTREE_PATH>
```

Then populate the top-level working tree in parallel:
```bash
git -C <WORKTREE_PATH> -c checkout.workers=$(nproc) -c checkout.thresholdForParallelism=1 checkout
```

### 5. Decide whether to set up submodules

The submodule hardlink step below is the slow part of this skill — it walks every contrib module, runs `cp -al` over all of them, rewrites configs, and checks out every submodule's working tree.

**Skip step 6 entirely (jump to step 7)** when the planned work is text-only and won't be built or tested locally:
- docs, comments, changelog entries, `.md` files
- typo fixes in source files where no compile is needed to verify

**Run step 6** when the task will involve `ninja` or running tests against this worktree.

If genuinely unsure, use `AskUserQuestion`: "Will you build or run tests in this worktree? (No → skip submodule setup, much faster)"

### 6. Set up submodules via hardlinks

Only do this step if step 5 selected the build/test path. Instead of cloning each submodule from the network, hardlink the git modules directory from the main repo. This gives each worktree an independent copy of the submodule git data (safe to modify independently) without using extra disk space for the object files, and without any network access.

Determine `GIT_DIR` — the `.git` directory of the main repo. For a regular repo this is `<MAIN_REPO>/.git`. For a worktree it may differ; use `git -C <MAIN_REPO> rev-parse --path-format=absolute --git-common-dir` to get the correct path. The `--path-format=absolute` flag is important: without it `--git-common-dir` returns a path relative to `<MAIN_REPO>` (just `.git`), which breaks every later `$GIT_DIR/...` reference the moment the shell's working directory is not `<MAIN_REPO>`. The flag must come before `--git-common-dir`.

Determine `WORKTREE_ENTRY` — the name git uses for this worktree's entry in `$GIT_DIR/worktrees/`. This is `$(basename <WORKTREE_PATH>)`.

```bash
GIT_DIR=$(git -C <MAIN_REPO> rev-parse --path-format=absolute --git-common-dir)
WORKTREE_ENTRY=$(basename <WORKTREE_PATH>)

# Hardlink-copy the modules directory from the main repo
cp -al $GIT_DIR/modules \
       $GIT_DIR/worktrees/$WORKTREE_ENTRY/modules

# Fix the worktree pointer inside each submodule's config.
# The hardlinked configs still reference the main repo's worktree path,
# so update them to point to the new worktree's contrib directories.
find $GIT_DIR/worktrees/$WORKTREE_ENTRY/modules -name config -exec \
    sed -i "s|worktree = .*/contrib/|worktree = <WORKTREE_PATH>/contrib/|" {} +

# Some submodules (e.g. contrib/boost) use the worktreeConfig extension,
# storing the actual core.worktree in config.worktree instead of config.
# The hardlinked config.worktree files contain relative paths like
# "../../../../contrib/boost" that resolve correctly from the main repo's
# modules dir but incorrectly from the worktree's modules dir.
# Fix them to use absolute paths pointing to the new worktree.
find $GIT_DIR/worktrees/$WORKTREE_ENTRY/modules -name config.worktree -exec \
    sed -i "s|worktree = .*/contrib/|worktree = <WORKTREE_PATH>/contrib/|" {} +

# Register and populate every submodule working tree, WITHOUT calling
# `git submodule update`. That command walks all ~130 submodules serially and takes
# ~25s even with everything already local (no network) — it is the bottleneck of
# this whole skill. Since the gitdirs are already hardlinked and their configs now
# point at this worktree's contrib dirs, all we need per submodule is to write its
# `.git` pointer file and check out the working tree.
#
# Two levels of parallelism are combined here. The outer `xargs -P` runs several
# submodules at once. The inner `checkout.workers` parallelises the file writes
# within a single submodule, which matters for the giant ones (llvm-project, aws,
# boost, rust_vendor): without it, one huge submodule checks out single-threaded and
# becomes the long pole while every other core sits idle. The outer-by-inner product
# is kept near the core count to avoid oversubscription. Submodules with no
# hardlinked gitdir are reported and skipped.
checkout_jobs=$(( $(nproc) / 8 )); [ "$checkout_jobs" -lt 1 ] && checkout_jobs=1
export submodule_modules_dir=$GIT_DIR/worktrees/$WORKTREE_ENTRY/modules
export worktree_path=<WORKTREE_PATH>
git -C <WORKTREE_PATH> config -f .gitmodules --get-regexp 'submodule\..*\.path' \
    | awk '{print $2}' \
    | xargs -P "$checkout_jobs" -I {} sh -c '
        submodule_path="$1"
        submodule_gitdir="$submodule_modules_dir/$submodule_path"
        submodule_tree="$worktree_path/$submodule_path"
        [ -d "$submodule_gitdir" ] || { echo "SKIP (no gitdir): $submodule_path"; exit 0; }
        mkdir -p "$submodule_tree"
        printf "gitdir: %s\n" "$submodule_gitdir" > "$submodule_tree/.git"
        git -C "$submodule_tree" -c checkout.workers=8 -c checkout.thresholdForParallelism=1 \
            read-tree -u --reset HEAD 2>/dev/null || echo "SKIP: $submodule_path"
      ' _ {}
```

**Important:** no `git submodule update` is needed because the hardlinked modules directory already contains every submodule's git data and the `sed` steps above already point each gitdir's `core.worktree` at this worktree. Writing the `.git` pointer file plus `git read-tree -u --reset HEAD` reproduces exactly what `submodule update` would do, but in parallel and with zero network access. The per-submodule SHA still matches what the superproject records, because each hardlinked gitdir's `HEAD` is the SHA the main repo had checked out.

The contrib working trees total roughly 11GB, so this phase is bound by disk write bandwidth, not CPU; the two-level parallelism keeps all cores busy but the floor is the time to physically write the bytes (about 3.5s on a fast NVMe). Hardlinking the working trees from the main repo would dodge that write and be much faster, but it is deliberately avoided: hardlinked working-tree files share inodes with the main repo, so any in-place edit (`sed -i`, an appended `>>`, an in-place codegen step) would silently corrupt the main repo and every sibling worktree. The git object database is safe to hardlink only because git objects are immutable.

This pass is non-recursive (top-level submodules only), which matches what the ClickHouse build needs. If you ever do need nested submodules, run `git -C <WORKTREE_PATH> submodule update --init --recursive` afterwards (slower, and may hit the network for any submodule not already present locally).

### 7. Report results

Report to the user:
- Source repo: `<MAIN_REPO>`
- Worktree path: `<WORKTREE_PATH>`
- Branch: `<branch-name>` (newly created or existing)
- Submodules: either "hardlinked from main repo (independent copies, no network cloning)" or "skipped (text-only task)" depending on the choice in step 5
- Suggest: `cd <WORKTREE_PATH>`

## Examples

- `/create-worktree my-feature` — Create a new worktree with a new branch `my-feature`
- `/create-worktree fix/issue-12345` — Create a new worktree, branch name can contain slashes (slashes are replaced with dashes in the default directory name)

## Notes

- For text-only tasks (docs, comments, typo fixes, changelog entries), step 6 is skipped — `git worktree add` alone is enough, and the worktree is ready immediately. If you later need to build there, run step 6's commands by hand.
- Submodules use hardlinks (`cp -al`) — git object files are hardlinked (no extra disk space) but each worktree has its own independent directory structure and config. Modifying submodules in one worktree does not affect others.
- The main repo must have submodules already cloned (`git submodule update --init` must have been run in the main repo at least once).
- To remove a worktree later:
  ```bash
  rm -rf <WORKTREE_PATH>
  git -C <MAIN_REPO> worktree prune
  ```
- Build directories are NOT shared — you'll need to set up CMake/build separately in the new worktree.
