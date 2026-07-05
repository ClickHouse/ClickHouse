---
name: create-worktree
description: Create a ClickHouse git worktree with submodules hardlinked from the main repo. Use when the user wants to create a new worktree for ClickHouse development.
argument-hint: <branch-name>
disable-model-invocation: false
allowed-tools: Bash(git:*), Bash(cp:*), Bash(ln:*), Bash(ls:*), Bash(rm:*), Bash(mkdir:*), Bash(find:*), Bash(pwd:*), Bash(sed:*), Bash(awk:*), Bash(xargs:*), Bash(sh:*), Bash(nproc:*), AskUserQuestion
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

**If branch exists locally:**
```bash
# Text-only:
git -C <MAIN_REPO> worktree add <WORKTREE_PATH> <branch-name>

# Build/test:
git -C <MAIN_REPO> worktree add --no-checkout <WORKTREE_PATH> <branch-name>
```

**If branch exists on remote only:**
```bash
# Text-only:
git -C <MAIN_REPO> worktree add <WORKTREE_PATH> -b <branch-name> origin/<branch-name>

# Build/test:
git -C <MAIN_REPO> worktree add --no-checkout <WORKTREE_PATH> -b <branch-name> origin/<branch-name>
```

**If branch does not exist (create new):**
```bash
# Text-only:
git -C <MAIN_REPO> worktree add -b <branch-name> <WORKTREE_PATH>

# Build/test:
git -C <MAIN_REPO> worktree add --no-checkout -b <branch-name> <WORKTREE_PATH>
```

### 5. Decide whether to set up submodules

The submodule hardlink step below is the slow part of this skill — it walks every contrib module, runs `cp -al` over all of them, rewrites configs, and checks out every submodule's working tree. The checkout repair treats `nproc` as a total worker budget: a small number of outer submodule jobs, each using parallel checkout workers internally.

**Skip step 6 entirely (jump to step 7)** when the planned work is text-only and won't be built or tested locally:
- docs, comments, changelog entries, `.md` files
- typo fixes in source files where no compile is needed to verify

**Run step 6** when the task will involve `ninja` or running tests against this worktree.

If genuinely unsure, use `AskUserQuestion`: "Will you build or run tests in this worktree? (No → skip submodule setup, much faster)"

### 6. Set up submodules via hardlinks

Only do this step if step 5 selected the build/test path. Instead of cloning each submodule from the network, hardlink the git modules directory from the main repo. This gives each worktree an independent copy of the submodule git data (safe to modify independently) without using extra disk space for the object files, and without any network access.

Determine `GIT_DIR` — the `.git` directory of the main repo. For a regular repo this is `<MAIN_REPO>/.git`. For a worktree it may differ; use `git -C <MAIN_REPO> rev-parse --git-common-dir` to get the correct path.

Determine `WORKTREE_ENTRY` — the name git uses for this worktree's entry in `$GIT_DIR/worktrees/`. This is `$(basename <WORKTREE_PATH>)`.

```bash
GIT_COMMON_DIR=$(git -C <MAIN_REPO> rev-parse --git-common-dir)
case "$GIT_COMMON_DIR" in
    /*) GIT_DIR=$GIT_COMMON_DIR ;;
    *) GIT_DIR=<MAIN_REPO>/$GIT_COMMON_DIR ;;
esac
WORKTREE_ENTRY=$(basename <WORKTREE_PATH>)

# Hardlink-copy the modules directory from the main repo while checking out the
# parent worktree files. These write disjoint paths, and the module copy only
# needs the worktree metadata created by `git worktree add --no-checkout`.
( cp -al $GIT_DIR/modules \
         $GIT_DIR/worktrees/$WORKTREE_ENTRY/modules ) &
cp_pid=$!

parent_checkout_status=0
git -C <WORKTREE_PATH> \
    -c checkout.workers=0 \
    -c core.fsync=none \
    -c gc.auto=0 \
    checkout -q -f HEAD -- . || parent_checkout_status=$?

modules_copy_status=0
wait "$cp_pid" || modules_copy_status=$?

if [ "$parent_checkout_status" -ne 0 ]
then
    printf "FAILED: parent checkout\n" >&2
    exit "$parent_checkout_status"
fi

if [ "$modules_copy_status" -ne 0 ]
then
    printf "FAILED: cp -al modules\n" >&2
    exit "$modules_copy_status"
fi

# Register submodules in the worktree config while rewriting module configs.
# These operations touch disjoint files: `submodule init` writes the worktree
# config, while the sed pass fixes copied submodule `config` and
# `config.worktree` files under the hardlinked modules dir.
git -C <WORKTREE_PATH> submodule init &
init_pid=$!

# Fix the worktree pointer inside each submodule's config and config.worktree
# files in one tree walk. Most modules use `config`; a few use the
# worktreeConfig extension and store the actual core.worktree in
# `config.worktree` instead.
find $GIT_DIR/worktrees/$WORKTREE_ENTRY/modules \
    \( -name config -o -name config.worktree \) -exec \
    sed -i "s|worktree = .*/contrib/|worktree = <WORKTREE_PATH>/contrib/|" {} +

if ! wait "$init_pid"
then
    printf "FAILED: submodule init\n" >&2
    exit 1
fi

CPU_COUNT=$(nproc)
DEFAULT_SUBMODULE_CHECKOUT_WORKERS=8
if [ "$DEFAULT_SUBMODULE_CHECKOUT_WORKERS" -gt "$CPU_COUNT" ]
then
    DEFAULT_SUBMODULE_CHECKOUT_WORKERS=$CPU_COUNT
fi

SUBMODULE_CHECKOUT_WORKERS=${SUBMODULE_CHECKOUT_WORKERS:-$DEFAULT_SUBMODULE_CHECKOUT_WORKERS}
if [ "$SUBMODULE_CHECKOUT_WORKERS" -lt 1 ]
then
    printf "FAILED: SUBMODULE_CHECKOUT_WORKERS must be positive\n" >&2
    exit 1
fi

SUBMODULE_JOBS=${SUBMODULE_JOBS:-$(( CPU_COUNT / SUBMODULE_CHECKOUT_WORKERS ))}
if [ "$SUBMODULE_JOBS" -lt 1 ]
then
    SUBMODULE_JOBS=1
fi

# This direct materialization path intentionally bypasses `git submodule update`.
# Refuse custom update commands because skipping those would change semantics.
( git -C <WORKTREE_PATH> config --file .gitmodules --get-regexp "^submodule\\..*\\.update$" 2>/dev/null || true ) |
    while IFS=" " read -r config_key update_command
    do
        case "$update_command" in
            "!"*)
                printf "FAILED: custom submodule update command is unsupported on local hardlink path: %s\n" "$config_key" >&2
                exit 1
                ;;
        esac
    done || exit 1

# Materialize submodule working trees in one parallel pass. Capture the gitlink
# commits once, sanity-check the count, then reuse the same list to feed each
# worker. If a commit is missing from the hardlinked module data, `git checkout`
# fails locally without fetching.
GITLINKS=$(git -C <WORKTREE_PATH> ls-files -s |
    sed -n "s/^160000 \([0-9a-f][0-9a-f]*\) 0[[:space:]]\(.*\)$/\1 \2/p")
GITLINK_COUNT=$(printf "%s\n" "$GITLINKS" | sed -n '$=')
SUBMODULE_COUNT=$(git -C <WORKTREE_PATH> config --file .gitmodules --get-regexp "^submodule\\..*\\.path$" |
    sed -n '$=')
if [ "${GITLINK_COUNT:-0}" != "${SUBMODULE_COUNT:-0}" ]
then
    printf "FAILED: gitlink count %s does not match .gitmodules count %s\n" "${GITLINK_COUNT:-0}" "${SUBMODULE_COUNT:-0}" >&2
    exit 1
fi

# Start known heavy submodules first, then emit the full gitlink list and keep
# the first occurrence of each path. This preserves largest-first scheduling
# without the per-submodule pack-size probing overhead.
{
    for submodule_path in \
        contrib/llvm-project \
        contrib/google-cloud-cpp \
        contrib/aws \
        contrib/openssl \
        contrib/icu \
        contrib/boost \
        contrib/rust_vendor \
        contrib/sysroot \
        contrib/grpc \
        contrib/arrow \
        contrib/curl \
        contrib/rocksdb \
        contrib/postgres \
        contrib/wasmtime
    do
        printf "%s\n" "$GITLINKS" |
            awk -v p="$submodule_path" '$2 == p { print; exit }'
    done

    printf "%s\n" "$GITLINKS"
} |
    awk "!seen[\$2]++ { print }" |
    while IFS=" " read -r expected_commit submodule_path
    do
        printf "%s\0%s\0" "$expected_commit" "$submodule_path"
    done |
    xargs -0 -r -n2 -P "$SUBMODULE_JOBS" sh -c '
        worktree_path=$1
        git_dir=$2
        worktree_entry=$3
        checkout_workers=$4
        expected_commit=$5
        submodule_path=$6

        if [ -z "$expected_commit" ] || [ -z "$submodule_path" ]
        then
            printf "FAILED: empty submodule checkout tuple\n" >&2
            exit 1
        fi

        module_git_dir="$git_dir/worktrees/$worktree_entry/modules/$submodule_path"
        module_worktree="$worktree_path/$submodule_path"

        mkdir -p "$module_worktree" || exit 1
        printf "gitdir: %s\n" "$module_git_dir" > "$module_worktree/.git" || exit 1

        if ! git --git-dir="$module_git_dir" --work-tree="$module_worktree" \
            -c advice.detachedHead=false \
            -c checkout.workers="$checkout_workers" \
            -c checkout.thresholdForParallelism=100 \
            -c index.threads=true \
            -c core.fsync=none \
            -c gc.auto=0 \
            checkout -q -f --detach "$expected_commit"
        then
            printf "FAILED: %s: commit %s is missing from the local mirror\n" "$submodule_path" "$expected_commit" >&2
            exit 1
        fi
    ' sh "<WORKTREE_PATH>" "$GIT_DIR" "$WORKTREE_ENTRY" "$SUBMODULE_CHECKOUT_WORKERS"

submodule_checkout_status=$?
if [ "$submodule_checkout_status" -ne 0 ]
then
    printf "ERROR: a submodule could not be checked out at the commit the superproject records (see the FAILED line above).\n" >&2
    printf "The local mirror is missing that commit, which happens when this worktree's branch pins a submodule\n" >&2
    printf "to a commit the main repo has never fetched. Fetch it in the main repo, then re-run step 6:\n" >&2
    printf "    git -C <MAIN_REPO> submodule update --init\n" >&2
    exit "$submodule_checkout_status"
fi
```

**Important:** `git submodule init` plus direct local checkout is used instead of `git submodule update --init` so the skill stays local-only. The submodule SHA is read from the superproject gitlink (`git ls-files -s`), so a worktree whose branch pins a submodule to a different commit than the source checkout still gets the correct dependency. If the hardlinked module data is incomplete, the command should fail; do not let `git` fetch or clone from the network on this path. If any submodule repair fails, the skill fails instead of silently skipping it, and prints the one command that fixes it (`git -C <MAIN_REPO> submodule update --init`, which fetches the missing commit into the shared mirror so a re-run succeeds offline). The default worker counts use `nproc` as a total budget: `SUBMODULE_CHECKOUT_WORKERS` defaults to `min(8, nproc)`, and `SUBMODULE_JOBS` defaults to `nproc / SUBMODULE_CHECKOUT_WORKERS`, clamped to at least 1.

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
- Submodules use hardlinks (`cp -al`) — git object files are hardlinked (no extra disk space) but each worktree has its own independent directory structure and config. Modifying submodules in one worktree does not affect others. Note this applies to the git object database only: the submodule working-tree files are checked out fresh (not hardlinked), so editing a contrib source in one worktree never touches another.
- The main repo must have submodules already cloned (`git submodule update --init` must have been run in the main repo at least once).
- To remove a worktree later (preferred):
  ```bash
  git -C <MAIN_REPO> worktree remove --force <WORKTREE_PATH>
  git -C <MAIN_REPO> worktree prune
  ```
- Or, if the directory is already gone:
  ```bash
  rm -rf <WORKTREE_PATH>
  git -C <MAIN_REPO> worktree prune
  ```
- Build directories are NOT shared — you'll need to set up CMake/build separately in the new worktree.
