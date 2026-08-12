---
description: 'Overview of the ClickHouse backport policy and automation'
sidebar_label: 'Backport System'
sidebar_position: 56
slug: /development/backports
title: 'Backport System'
doc_type: 'reference'
---

This document describes the ClickHouse backport policy and the automated system that implements it.

## Release Model {#release-model}

ClickHouse versions follow the scheme `YY.M.patch.build-type`, where `YY` is the two-digit year, `M` is the release month (no leading zero), `patch` is the patch number within the branch, `build` is a monotonically increasing build number, and `type` is either `stable` or `lts`.

Example: `25.3.8.23-lts` — March 2025 LTS, patch 8, build 23.

There are two release tracks:

- **Stable** releases are published roughly monthly. The three most recent stable releases receive patches, giving approximately three months of active support per release.
- **LTS (Long-Term Support)** releases are published in March and August each year. Two LTS versions are supported simultaneously, each for at least 12 months.

Users running production workloads are encouraged to use either the latest stable or an LTS release, and to upgrade to new patch versions promptly, as patch releases do not introduce breaking changes.

## Backport Policy {#backport-policy}

Not all changes are backported. The goal is to keep release branches stable, so the scope of backports is intentionally narrow:

- **Security fixes** — always backported.
- **Critical bug fixes** (exceptions (logical errors), data loss, wrong results, RBAC issues) — automatically selected for backporting under the general backport rules; identified by the `pr-critical-bugfix` label, which causes `pr-must-backport` to be added automatically.
- **Stability and regression fixes** — backported when the risk of the change is low relative to the risk of leaving the bug in place; identified by `pr-must-backport` added manually by maintainers.
- **Minor bug fixes with an available workaround** — generally not backported to avoid destabilising release branches.
- **New features, improvements, performance work** — not backported.

The label `pr-must-backport` is the manual override used by maintainers to mark a PR for backporting. The label `pr-critical-bugfix` causes `pr-must-backport` to be added automatically by the CI hook (see `pr_labels_and_category.py`).

**Conflict escalation.** When automatic backporting cannot resolve merge conflicts, a cherry-pick PR must still be created and assigned to the author, merger, and existing assignees of the original PR so that a human can resolve the conflicts and complete the backport.

## Backport Tool {#backport-tool}

The backport policy described above is implemented by the automated tool in `tests/ci/cherry_pick.py`. The tool runs as a GitHub Actions workflow on ClickHouse infrastructure and covers all the requirements: discovering active release branches, selecting PRs that qualify for backporting, performing the two-stage cherry-pick and backport procedure, managing conflicts, enforcing the delay policy, and keeping labels in sync.

The long-term goal is to extract this implementation into a standalone open-source Python tool that other projects can adopt. The target design is:

- **Configurable** — all policy parameters (qualifying labels, delay window, stale PR thresholds, rolling-out behaviour, etc.) expressed as a configuration file so the tool can be adapted to match any project's backport requirements without code changes.
- **Distributable** — packaged as a self-contained Python wheel installable from PyPI, with no dependency on ClickHouse's CI infrastructure.
- **Programmable** — exposing a clean object model for pull requests, labels, and release branches so that users can script custom workflows on top of the core engine.

### Testing {#testing}

A planned part of the standalone tool is a dedicated test suite together with a lightweight testing infrastructure. The infrastructure will be able to spin up temporary GitHub repositories (or local equivalents) pre-populated with:

- a configurable set of branches representing release lines,
- pull requests carrying various combinations of backport labels,
- release PRs with the `release` label pointing at the release branches.

This lets tests exercise the full automation loop — label detection, cherry-pick branch creation, conflict handling, backport PR creation, assignee logic, rolling-out skip, and delay policy — against a real but disposable repository, without touching production state. The same infrastructure can be reused to regression-test policy changes before they are deployed.

## Active Release Branches {#active-release-branches}

An active release branch is any branch whose corresponding release PR (carrying the `release` label) is still open on GitHub. The backport automation discovers these dynamically on each run, so no configuration changes are needed when a new release is cut or an old one reaches end-of-life.

A release branch can be in a **rolling-out** state (its release PR carries the `rolling-out` label) during the period when a new release is being deployed. General backports are paused for rolling-out branches to avoid complicating the rollout. Version-specific labels (e.g. `v25.3-must-backport`) override this and force backporting even during a rollout.

## Implementation {#implementation}

### Overview {#overview}

The backport automation runs hourly as the `CherryPick` GitHub Actions workflow (`.github/workflows/cherry_pick.yml`), implemented in `tests/ci/cherry_pick.py`. It operates through the GitHub API and local git operations on a self-hosted `style-checker-aarch64` runner.

The process is two-stage for each (original PR, release branch) pair:

1. A **cherry-pick PR** is created to isolate conflict resolution from the actual merge target. If there are no conflicts, it is merged automatically.
2. A **backport PR** is created against the real release branch, with the cherry-picked changes squashed into a single commit.

### Labels {#labels}

Labels on the original PR control whether and where backporting happens.

| Label | Effect |
|---|---|
| `pr-must-backport` | Backport to all active release branches (skipping branches marked `rolling-out`) |
| `pr-must-backport-force` | Backport to all active release branches, ignoring `rolling-out` restrictions |
| `pr-critical-bugfix` | Triggers `pr-must-backport` automatically (via `AUTO_BACKPORT` in `pr_labels_and_category.py`) |
| `v{VER}-must-backport` (e.g. `v25.3-must-backport`) | Backport only to that specific release branch; overrides `rolling-out` skip for that branch |
| `pr-backports-created` | Set by the bot when all required backport PRs have been created; cleared if a cherry-pick PR is reopened |
| `pr-cherrypick` | Applied to cherry-pick PRs created by the bot |
| `pr-backport` | Applied to backport PRs created by the bot |
| `do not test` | Applied to cherry-pick PRs so CI does not run on them |
| `rolling-out` | Set on a **release PR** to indicate its branch is currently being rolled out; general backports skip it |

### Branch and PR Naming {#branch-and-pr-naming}

For each original PR number `N` and release branch `release/X.Y`:

- Cherry-pick branch: `cherrypick/release/X.Y/N`
- Backport branch: `backport/release/X.Y/N`
- Cherry-pick PR title: `Cherry pick #N to release/X.Y: <original title>`
- Backport PR title: `Backport #N to release/X.Y: <original title>`

### Step-by-Step Process {#step-by-step-process}

#### 1. Discover active releases {#discover-active-releases}

`BackportPRs.receive_release_prs` queries GitHub for all open PRs with the `release` label. The head refs of these PRs are the release branch names (e.g. `release/25.3`). A compatibility label set is derived from them: `v25.3-must-backport`, etc.

#### 2. Find PRs to backport {#find-prs-to-backport}

`BackportPRs.receive_prs_for_backport` uses the GitHub search API to find merged PRs that:
- carry at least one backport label (`pr-must-backport`, `pr-must-backport-force`, `pr-critical-bugfix`, or a version-specific label), and
- do **not** already have `pr-backports-created`, and
- were merged after the oldest commit date found on any release branch, and
- were updated within the last 90 days (to keep the search query efficient).

#### 3. Rolling-out branch handling {#rolling-out-branch-handling}

When a release PR carries the `rolling-out` label, general backport labels (`pr-must-backport`, `pr-critical-bugfix`) skip that branch. The bot closes any previously created cherry-pick or backport PRs for that branch with an explanatory comment. A version-specific label (e.g. `v25.3-must-backport`) always overrides this. `pr-must-backport-force` bypasses the `rolling-out` check for all branches.

#### 4. Cherry-pick stage (`ReleaseBranch.create_cherrypick`) {#cherry-pick-stage}

For each (original PR, release branch) pair where no cherry-pick PR exists yet:

1. Check out the release branch and create a **backport branch** (`backport/release/X.Y/N`) from it.
2. Perform `git merge -s ours` against the first parent of the merge commit to create a synthetic merge base with no content changes.
3. Force-create a **cherry-pick branch** (`cherrypick/release/X.Y/N`) pointing directly at the merge commit of the original PR.
4. Attempt `git merge --no-commit --no-ff` of the cherry-pick branch into the backport branch:
   - If already up-to-date, the change is already present in the release branch — mark as done and skip.
   - Otherwise (with or without conflicts) reset and push both branches.
5. Create the cherry-pick PR targeting `backport/release/X.Y/N` from `cherrypick/release/X.Y/N`, labelled `pr-cherrypick` and `do not test`.
6. Propagate `pr-bugfix` or `pr-critical-bugfix` from the original PR if applicable.
7. Assignees are **not** set at this point; they are only added when conflicts are detected.

#### 5. Auto-merge of conflict-free cherry-pick PRs {#auto-merge-conflict-free-cherry-pick-prs}

If the cherry-pick PR is mergeable (no conflicts), the bot merges it automatically via the GitHub API and proceeds immediately to the backport stage.

#### 6. Backport stage (`ReleaseBranch.create_backport`) {#backport-stage}

After the cherry-pick PR is merged:

1. Check out and pull the backport branch.
2. Find the merge-base between the release branch and the backport branch.
3. `git reset --soft` to the merge-base, squashing all cherry-picked commits into one.
4. Commit with the backport PR title as the message.
5. Force-push the backport branch and open a backport PR targeting the real release branch.
6. Label the PR `pr-backport` (and `pr-bugfix` / `pr-critical-bugfix` if applicable).
7. Assign the PR to the original PR's author, merger, and existing assignees (excluding robot accounts).

#### 7. Completion {#completion}

When all release branches for a given original PR are backported, the bot adds `pr-backports-created` to the original PR.

#### 8. Pre-check {#pre-check}

Before starting any work on a PR, `ReleaseBranch.pre_check` runs `git merge-base --is-ancestor` to verify the merge commit is not already reachable from the release branch. If it is, the PR is considered already backported and skipped.

### Stale Cherry-pick PR Handling {#stale-cherry-pick-pr-handling}

The `CherryPickPRs` class runs at the start of each hourly execution and handles two scenarios:

- **Orphaned cherry-pick PRs**: If a cherry-pick PR's release branch no longer has an open release PR (i.e. the release is closed), the cherry-pick PR is closed automatically.
- **Reopened cherry-pick PRs**: If an original PR already has `pr-backports-created` but a cherry-pick PR for it is still open, the `pr-backports-created` label is removed from the original PR so it can be reprocessed.

For cherry-pick PRs waiting for manual conflict resolution:
- After **3 days** of no updates, the bot posts a ping comment mentioning the assignees.
- After **7 days** of no updates, the bot posts a closing comment and closes the PR.

### Conflict Resolution {#conflict-resolution}

When a cherry-pick has conflicts, the cherry-pick PR is left open for a human to resolve. The bot assigns it to the original PR's author, merger, and assignees. After the conflicts are resolved and the cherry-pick PR is merged, the bot creates the backport PR on the next hourly run.

To discard a backport entirely, close the cherry-pick PR. The bot will treat it as intentionally skipped.

To recreate a broken cherry-pick PR from scratch:
1. Remove the `pr-cherrypick` label from the cherry-pick PR.
2. Delete the `cherrypick/...` branch.
3. Remove `pr-backports-created` from the original PR if present.

### CI for Backport PRs {#ci-for-backport-prs}

Backport PRs target release branches, so they use a dedicated CI workflow (`BackportPR`, defined in `ci/workflows/backport_branches.py`) rather than the standard pull request workflow. This workflow runs a representative subset of CI: ASan/UBSan and TSan builds, release builds, macOS builds, functional tests under ASan, stress tests under TSan, and integration tests. It validates that the backport branch has between 1 and 50 commits and at least one changed file (enforced by `check_backport_branch.py`).

### Authentication {#authentication}

The workflow uses an SSH key (`ROBOT_CLICKHOUSE_SSH_KEY`) for git push operations. GitHub API calls are authenticated via `get_best_robot_token`, which selects the token with the most remaining quota from a pool stored in SSM (`/github-tokens`). `ROBOT_CLICKHOUSE_COMMIT_TOKEN` is used by the checkout step in the Actions workflow, not for API calls. Robot accounts (`robot-clickhouse`, `clickhouse-gh`) are excluded when assigning a responsible person.

### GitHub API Cache {#github-api-cache}

`GitHubCache` (from `cache_utils.py`) persists the PyGithub object cache to S3, reducing API calls across hourly runs. The cache is downloaded at the start and uploaded at the end of each run.

### Error Handling {#error-handling}

Errors during individual PR processing are caught and logged but do not stop the run. After all PRs are processed, if any errors occurred, a `BackportException` is raised. In CI, this triggers a notification via `CIBuddy` to the team chat.
