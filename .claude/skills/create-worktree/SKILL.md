---
name: create-worktree
description: Create a ClickHouse git worktree with submodules hardlinked from the main repo. Use when the user wants to create a new worktree for ClickHouse development.
argument-hint: <branch-name>
disable-model-invocation: false
allowed-tools: Bash(git:*), Bash(cp:*), Bash(ln:*), Bash(ls:*), Bash(rm:*), Bash(mkdir:*), Bash(find:*), Bash(pwd:*), AskUserQuestion
---

# Create ClickHouse Worktree Skill

Create a new git worktree for ClickHouse development with submodules hardlinked from the main repo (independent copies, no network, no extra disk for git objects).

## Arguments

- `$0` (required): Branch name. If the branch already exists, the worktree will check it out. If it doesn't exist, a new branch will be created from the current HEAD of the main repo.

## Process

### 1. Determine the source repo

- Detect the source repo (`MAIN_REPO`) by running `git rev-parse --show-toplevel` from the current working directory.
- Verify it is a git repository. If not, report an error and stop.

### 2. Validate inputs

- Ensure `$0` (branch name) is provided. If not, use `AskUserQuestion` to ask the user for a branch name.
- Use `AskUserQuestion` to ask the user for the **worktree destination path**. Suggest `<MAIN_REPO>/../<MAIN_REPO_NAME>-<branch-name>` as the default — this places the worktree as a sibling of the main repo directory, named after both the repo and the branch (e.g. `../ClickHouse-my-feature`). The user may enter a different path if preferred.

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

**If branch exists locally:**
```bash
git -C <MAIN_REPO> worktree add <WORKTREE_PATH> <branch-name>
```

**If branch exists on remote only:**
```bash
git -C <MAIN_REPO> worktree add <WORKTREE_PATH> -b <branch-name> origin/<branch-name>
```

**If branch does not exist (create new):**
```bash
git -C <MAIN_REPO> worktree add -b <branch-name> <WORKTREE_PATH>
```

### 5. Set up submodules via hardlinks

This is the key optimization — instead of cloning each submodule from the network, hardlink the git modules directory from the main repo. This gives each worktree an independent copy of the submodule git data (safe to modify independently) without using extra disk space for the object files, and without any network access.

Determine `GIT_DIR` — the `.git` directory of the main repo. For a regular repo this is `<MAIN_REPO>/.git`. For a worktree it may differ; use `git -C <MAIN_REPO> rev-parse --git-common-dir` to get the correct path.

```bash
GIT_DIR=$(git -C <MAIN_REPO> rev-parse --git-common-dir)

# Hardlink-copy the modules directory from the main repo
cp -al $GIT_DIR/modules \
       $GIT_DIR/worktrees/<branch-name>/modules

# Fix the worktree pointer inside each submodule's config.
# The hardlinked configs still reference the main repo's worktree path,
# so update them to point to the new worktree's contrib directories.
find $GIT_DIR/worktrees/<branch-name>/modules -name config -exec \
    sed -i "s|worktree = .*/contrib/|worktree = <WORKTREE_PATH>/contrib/|" {} +

# Some submodules (e.g. contrib/boost) use the worktreeConfig extension,
# storing the actual core.worktree in config.worktree instead of config.
# The hardlinked config.worktree files contain relative paths like
# "../../../../contrib/boost" that resolve correctly from the main repo's
# modules dir but incorrectly from the worktree's modules dir.
# Fix them to use absolute paths pointing to the new worktree.
find $GIT_DIR/worktrees/<branch-name>/modules -name config.worktree -exec \
    sed -i "s|worktree = .*/contrib/|worktree = <WORKTREE_PATH>/contrib/|" {} +

# Register submodules and write .git pointer files into contrib/ directories
# (no network — uses hardlinked objects). This does NOT populate working trees.
git -C <WORKTREE_PATH> submodule update

# Populate submodule working trees. The previous command only writes .git pointer
# files but leaves working trees empty because the hardlinked index files are empty.
# We must explicitly reset each submodule's index from HEAD and checkout the files.
# Some submodules may fail if their git data is incomplete — safe to skip.
git -C <WORKTREE_PATH> submodule foreach \
    '(git read-tree HEAD && git checkout -- .) 2>/dev/null || echo "SKIP: $name"'
```

**Important:** `git submodule update` (without `--init`) is sufficient for the first step because the hardlinked modules directory already contains all the submodule git data. Everything is purely local — no network access occurs. However, it only creates `.git` pointer files in each `contrib/` subdirectory without populating the working trees. The subsequent `git read-tree HEAD && git checkout -- .` in each submodule is required to actually check out the files.

If `git submodule update` fails with errors about uninitialized submodules, run:
```bash
git -C <WORKTREE_PATH> submodule init
git -C <WORKTREE_PATH> submodule update
```

### 6. Report results

Report to the user:
- Source repo: `<MAIN_REPO>`
- Worktree path: `<WORKTREE_PATH>`
- Branch: `<branch-name>` (newly created or existing)
- Submodules: hardlinked from main repo (independent copies, no network cloning)
- Suggest: `cd <WORKTREE_PATH>`

## Examples

- `/create-worktree my-feature` — Create a new worktree with a new branch `my-feature`
- `/create-worktree fix/issue-12345` — Create a new worktree, branch name can contain slashes

## Notes

- Submodules use hardlinks (`cp -al`) — git object files are hardlinked (no extra disk space) but each worktree has its own independent directory structure and config. Modifying submodules in one worktree does not affect others.
- The main repo must have submodules already cloned (`git submodule update --init` must have been run in the main repo at least once).
- To remove a worktree later:
  ```bash
  rm -rf <WORKTREE_PATH>
  git -C <MAIN_REPO> worktree prune
  ```
- Build directories are NOT shared — you'll need to set up CMake/build separately in the new worktree.
