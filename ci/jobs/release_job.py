"""ClickHouse release pipeline job.

INVARIANT: every run starts in a clean, empty GitHub Actions `_work` directory -
the runner is ephemeral and the workspace is a fresh `actions/checkout` (a depth-1
shallow clone). There is NO state carried over from a previous run. So do not add
"in case a previous run left X on a reused runner" defenses here: there is no
reuse. The repo is always shallow at the start (hence the unconditional
`--unshallow`), and no leftover files/branches/credentials can exist.
"""

import argparse
import json
import os
import re
import shlex
import shutil
import tempfile
from pathlib import Path
from typing import List, Tuple

from ci.praktika.gh import GH
from ci.praktika.git import Git
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.secret import Secret
from ci.praktika.utils import Shell, Utils

_GH_TOKEN_SECRET = Secret.Config(
    name="/github-tokens/robot-1",
    type=Secret.Type.AWS_SSM_PARAMETER,
)
# Docker Hub robot credentials for pushing release images. The legacy
# `release-maker` runner was logged in to Docker Hub out-of-band; the
# ephemeral `release-maker-asg` runners are not, so the registry push must
# authenticate explicitly (mirrors `docker_login` in ci/jobs/docker_server.py).
_DOCKERHUB_USERNAME = "robotclickhouse"
_DOCKERHUB_SECRET = Secret.Config(
    name="dockerhub_robot_password",
    type=Secret.Type.AWS_SSM_PARAMETER,
)

_GEESEFS_VERSION = "v0.43.5"

# binfmt is run as a --privileged container in the release job, so pin it by
# digest (not the mutable `latest` tag) to avoid executing a moved/tampered
# image with elevated privileges on the self-hosted release runner.
_BINFMT_IMAGE = (
    "tonistiigi/binfmt@sha256:"
    "400a4873b838d1b89194d982c45e5fb3cda4593fbfd7e08a02e76b03b21166f0"
)

_R2_AUTH_TEST_SECRET = Secret.Config(
    name="/release/r2-auth-test",
    type=Secret.Type.AWS_SSM_PARAMETER,
)
_R2_AUTH_PROD_SECRET = Secret.Config(
    name="/release/r2-auth",
    type=Secret.Type.AWS_SSM_PARAMETER,
)
_GPG_SIGNING_KEY_SECRET = Secret.Config(
    name="/release/gpg-signing-key",
    type=Secret.Type.AWS_SSM_SECRET,
)

REPO_PATH = Utils.cwd()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Runs the ClickHouse release pipeline",
    )
    parser.add_argument(
        "--ref",
        type=str,
        default=None,
        help="Git reference (branch or commit sha) from which the release was created",
    )
    parser.add_argument(
        "--release-type",
        choices=("new", "patch"),
        default=None,
        help="The type of release",
    )
    parser.add_argument(
        "--assignee",
        type=str,
        default=None,
        help="GitHub login to assign the changelog PR to",
    )
    parser.add_argument(
        "--only-repo",
        action="store_true",
        help="Run only repo updates (skip tag push, branch push, version bump)",
    )
    parser.add_argument(
        "--only-docker",
        action="store_true",
        help="Run only docker builds (skip tag push, branch push, version bump)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not make any actual changes, just show what will be done",
    )
    args = parser.parse_args()

    # When CLI args are absent, fall back to workflow inputs (CI runs).
    # workflow_dispatch passes strings; workflow_call passes native types
    # (e.g. booleans), so coerce to str before the callers do `.lower()`.
    def _wi(name: str) -> str:
        value = Info.get_workflow_input_value(name)
        return "" if value is None else str(value)

    if args.ref is None:
        args.ref = _wi("ref")
    if args.release_type is None:
        args.release_type = _wi("type") or None
    if not args.dry_run:
        args.dry_run = _wi("dry-run").lower() == "true"
    if not args.only_repo:
        args.only_repo = _wi("only-repo").lower() == "true"
    if not args.only_docker:
        args.only_docker = _wi("only-docker").lower() == "true"
    if args.assignee is None:
        args.assignee = _wi("assignee")

    assert args.ref, "ref must be set via --ref or workflow dispatch input 'ref'"
    assert args.release_type in (
        "new",
        "patch",
    ), "release-type must be 'new' or 'patch'"

    return args


RELEASE_INFO_FILE = "/tmp/release_info.json"


def main():
    stopwatch = Utils.Stopwatch()
    args = parse_args()

    # Drop any release-info file left by a previous release on a reused
    # self-hosted runner. "Prepare Release Info" writes a fresh STARTED stub as
    # soon as it runs, so from here on RELEASE_INFO_FILE exists only if that step
    # ran this attempt; the "--post-status" step below is guarded on it, so an
    # early setup failure that skips prepare neither reads a missing file
    # (FileNotFoundError) nor reports a stale previous release's status.
    if os.path.exists(RELEASE_INFO_FILE):
        os.remove(RELEASE_INFO_FILE)

    dry_run_flag = "--dry-run" if args.dry_run else ""
    original_branch = Shell.get_output("git rev-parse --abbrev-ref HEAD", strict=True)
    # Per-run GNUPGHOME for the signing key (set when the GPG import step runs);
    # removed by the cleanup step so the private key never persists on a reused
    # runner.
    gnupg_home = None
    # Per-run DOCKER_CONFIG so every docker invocation (login, buildx, push)
    # writes auth/state into an isolated directory instead of the runner user's
    # ~/.docker/config.json; removed by the cleanup step so the robot registry
    # credentials never persist for a later job on a reused runner.
    docker_config = tempfile.mkdtemp(prefix="release-docker-")
    os.chmod(docker_config, 0o700)
    os.environ["DOCKER_CONFIG"] = docker_config

    # Export the robot token once for the whole job (the legacy workflow set it
    # as a job-level `env: GH_TOKEN`). Commands then reference `$GH_TOKEN` instead
    # of interpolating the secret value, so praktika's verbose command logging
    # (Shell.run prints every command) never writes the token to the job log. The
    # robot PAT carries the `workflow` scope, so pushes of tags/branches whose
    # `.github/workflows` differ from master are not rejected by GitHub's
    # push-time workflow-scope check (which the App token, lacking that scope,
    # cannot pass on a repo this large).
    os.environ["GH_TOKEN"] = _GH_TOKEN_SECRET.get_value()

    results = []
    ok = True

    def step(**kwargs):
        nonlocal ok
        if not ok:
            return
        results.append(Result.from_commands_run(**kwargs))
        if results[-1].status != Result.Status.OK:
            ok = False

    step(
        name="Fetch Repository History (treeless)",
        command=[
            # This job only needs full commit history - for the version tweak
            # (commit count since the previous tag), changelog.py, and the
            # all-time contributors `git shortlog` - not the file contents of that
            # history. A treeless partial clone (`--filter=tree:0`) fetches every
            # commit but no trees/blobs, so the unshallow is far cheaper than a
            # full one; any tree/blob a later step actually reads is fetched
            # lazily from the promisor remote.
            #
            # No `|| true`: the runner is ephemeral, so the checkout is always the
            # fresh depth-1 shallow clone and --unshallow always applies - a
            # failure here is real and must fail the step, not be swallowed.
            #
            # `--quiet` on all three: without it fetch prints a "[new ref]" line
            # per branch/tag, which floods the log for a repo with this many refs.
            "git fetch --quiet --filter=tree:0 --unshallow --no-recurse-submodules origin",
            # actions/checkout configures origin to fetch only the workflow ref,
            # but prepare needs origin/<release_branch> (and origin/master); fetch
            # all heads so those refs are present regardless of the release branch.
            "git fetch --quiet --no-recurse-submodules origin '+refs/heads/*:refs/remotes/origin/*'",
            "git fetch --quiet --tags --no-recurse-submodules origin",
        ],
        workdir=REPO_PATH,
    )

    step(
        name="Configure Git Auth for Release Pushes",
        command=[
            # The checkout step authenticates `origin` with the default
            # GITHUB_TOKEN through an http extraheader. Release pushes (tags,
            # the new release branch, the version-bump branch) must use the
            # robot token instead so they carry the right permissions and
            # trigger downstream workflows such as ReleaseBranchCI. Drop the
            # extraheader and let gh's credential helper supply $GH_TOKEN.
            "git config --unset-all http.https://github.com/.extraheader || true",
            "gh auth setup-git",
        ],
        workdir=REPO_PATH,
    )

    # Authenticate to Docker Hub in the setup phase, before any release
    # mutation (tag push, GitHub release, repo export). Pushing docker images
    # is part of the release contract, so a missing/expired registry token must
    # stop the run before partial publication. Gated on patch && !dry_run so it
    # also covers only-repo / only-docker recovery runs.
    if args.release_type == "patch" and not args.dry_run:

        def docker_login():
            Shell.check(
                f"docker login --username {shlex.quote(_DOCKERHUB_USERNAME)}"
                f" --password-stdin",
                strict=True,
                stdin_str=_DOCKERHUB_SECRET.get_value(),
                encoding="utf-8",
            )

        step(
            name="Docker Hub Login",
            command=docker_login,
            workdir=REPO_PATH,
        )

    if args.release_type == "patch" and not args.only_docker:
        arch = "amd64" if Shell.get_output("uname -m") == "x86_64" else "arm64"
        geesefs_bin_dir = os.path.expanduser("~/.local/bin")
        os.makedirs(geesefs_bin_dir, exist_ok=True)
        if geesefs_bin_dir not in os.environ.get("PATH", ""):
            os.environ["PATH"] = geesefs_bin_dir + os.pathsep + os.environ.get("PATH", "")
        step(
            name="Install geesefs",
            command=[
                f"command -v geesefs && geesefs --version 2>&1 | grep -qF {_GEESEFS_VERSION.lstrip('v')} ||"
                f" (curl -fsSL https://github.com/yandex-cloud/geesefs/releases/download/{_GEESEFS_VERSION}/geesefs-linux-{arch}"
                f" -o {geesefs_bin_dir}/geesefs && chmod +x {geesefs_bin_dir}/geesefs)",
                # `apt-get update` before each install: the runner's apt index can
                # be stale and point at a superseded package version (e.g.
                # `libarchive-dev 3.6.0-1ubuntu1.7`) that Ubuntu already removed
                # from the mirror pool, which makes `apt-get install` fail with a
                # 404. Refreshing the index first resolves to the current version.
                "command -v createrepo_c || (sudo apt-get update && sudo apt-get install -y createrepo-c) ||:",
                # reprepro 5.4.4+ is required for the 'Limit' field in distributions config.
                # Ubuntu Jammy only has 5.3.0, so build from source if needed.
                "reprepro --version 2>&1 | grep -qE '5\\.[4-9]' || ("
                "  sudo apt-get update &&"
                "  sudo apt-get install -y dpkg-dev fakeroot libgpgme-dev libdb-dev libbz2-dev liblzma-dev libarchive-dev shunit2 db-util debhelper &&"
                "  git clone https://salsa.debian.org/debian/reprepro.git /tmp/reprepro-src &&"
                "  cd /tmp/reprepro-src &&"
                "  dpkg-buildpackage -b --no-sign &&"
                "  sudo dpkg -i ../reprepro_$(dpkg-parsechangelog --show-field Version)_$(dpkg-architecture -q DEB_HOST_ARCH).deb"
                ") ||:",
            ]
            # The installs above are best-effort (`||:`) so a local dev machine
            # without sudo/apt is not blocked. For a real release the repo tools
            # must be present before any mutation (tags, GitHub release, repos),
            # so verify them here and fail closed. Skipped on dry-run (local
            # convenience).
            + (
                []
                if args.dry_run
                else [
                    # Verify the *version*, not just presence: an older
                    # distro reprepro (5.3.x) may be installed while the 5.4+
                    # source build failed under the trailing `||:`. reprepro
                    # 5.4+ is required (the 'Limit' distributions field).
                    "command -v createrepo_c >/dev/null"
                    " && reprepro --version 2>&1 | grep -qE '5\\.[4-9]'"
                    " || { echo 'ERROR: createrepo_c and reprepro 5.4+ must be"
                    " installed for a release' >&2; exit 1; }"
                ]
            ),
            workdir=REPO_PATH,
        )

        def _write_secret_file(path: str, content: str) -> None:
            # These hold R2 package-publishing credentials; create them 0600 so
            # they are not exposed to other users/processes on a (reused)
            # self-hosted runner, regardless of the umask. They are removed by
            # the cleanup step at the end of the job.
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w") as f:
                f.write(content)
            os.chmod(path, 0o600)

        def write_r2_auth():
            _write_secret_file(
                os.path.expanduser("~/.r2_auth_test"), _R2_AUTH_TEST_SECRET.get_value()
            )
            if not args.dry_run:
                _write_secret_file(
                    os.path.expanduser("~/.r2_auth"), _R2_AUTH_PROD_SECRET.get_value()
                )

        step(
            name="Write R2 Auth Config",
            command=write_r2_auth,
            workdir=REPO_PATH,
        )

        # Import the signing key into a per-run GNUPGHOME (0700) rather than the
        # runner user's default keyring, and export it so reprepro signing in
        # the export steps uses the same home. The directory is deleted by the
        # cleanup step, so the private key does not persist for later jobs on a
        # reused runner.
        gnupg_home = tempfile.mkdtemp(prefix="release-gnupg-")
        os.chmod(gnupg_home, 0o700)
        os.environ["GNUPGHOME"] = gnupg_home

        def import_gpg_key():
            import base64
            key_b64 = _GPG_SIGNING_KEY_SECRET.get_value()
            key_data = base64.b64decode(key_b64)
            with tempfile.NamedTemporaryFile(delete=False, suffix=".gpg") as f:
                f.write(key_data)
                key_file = f.name
            try:
                Shell.check(f"gpg --import {key_file}", strict=True)
            finally:
                os.unlink(key_file)

        step(
            name="Import GPG Signing Key",
            command=import_gpg_key,
            workdir=REPO_PATH,
        )

    step(
        name="Prepare Release Info",
        command=[
            f"python3 ./ci/jobs/scripts/create_release.py --prepare-release-info"
            f" --ref {shlex.quote(args.ref)} --release-type {args.release_type}"
            f" {dry_run_flag}".strip()
        ],
        workdir=REPO_PATH,
    )

    # Prepare decides whether this run creates a new release (push tag, bump
    # version, changelog PR) or only re-publishes artifacts for an existing /
    # out-of-order ref. The creation steps below run only when it does; a
    # recovery (only-repo/only-docker) or an out-of-order full run skips them
    # without erroring and just re-exports repos / rebuilds docker.
    create_new_release = False
    if ok:
        with open(RELEASE_INFO_FILE) as f:
            create_new_release = json.load(f)["create_new_release"]

    # only-repo / only-docker only re-publish artifacts for an already-created
    # release (repo/Docker recovery). If the ref resolves to a new release, they
    # would otherwise fall through to the creation steps below (push tag, bump
    # version, PRs) and produce a partial new release, so reject that misuse and
    # require the release tag instead.
    if ok and create_new_release and (args.only_repo or args.only_docker):

        def _require_recovery_ref():
            raise RuntimeError(
                "only-repo/only-docker re-publish an existing release and must be "
                "run against its release tag (recovery); the given ref resolves to "
                "a new release. Pass the release tag as the ref."
            )

        step(name="Validate Recovery Ref", command=_require_recovery_ref)

    # The release opens exactly one PR against master - the changelog PR for a
    # patch, the version-bump PR for a new release. Ensure it exists and is
    # merged, idempotently: create it if absent, merge it if open, skip if it is
    # already merged. This converges a fresh release and a recovery / rerun after
    # a failed create-or-merge through the same path. These PR operations key off
    # the PR's actual state, not the run mode, so they run regardless of
    # only-repo/only-docker: a cheap recovery run can create a missing release PR
    # or enqueue an open-but-unmerged one (e.g. when the original run's enqueue
    # lost the race with a still-pending `CH Inc sync` required check).
    if args.dry_run:
        # No gh reads on dry-run (it may be a local run without gh auth): fall
        # back to the fresh-release signal so the generation is still previewed.
        release_pr_absent = create_new_release
        release_pr_needs_merge = create_new_release
    else:
        release_pr_branch = None
        release_pr_state = ""  # "MERGED" | "OPEN" | ""
        if ok:
            with open(RELEASE_INFO_FILE) as f:
                _info = json.load(f)
            release_pr_branch = (
                f"auto/{_info['release_tag']}"
                if args.release_type == "patch"
                else f"bump_version_{_info['version']}"
            )
            release_pr_state = GH.get_pr_state_by_branch(
                release_pr_branch, "ClickHouse/ClickHouse"
            )
            print(
                f"Release PR branch [{release_pr_branch}] state: "
                + (release_pr_state or "absent — will create")
            )
        release_pr_absent = release_pr_branch is not None and release_pr_state == ""
        release_pr_needs_merge = (
            release_pr_branch is not None and release_pr_state != "MERGED"
        )

    # Fail-fast: verify the release packages exist (this downloads them) before
    # pushing the tag or opening the changelog PR, so a missing-artifacts run
    # aborts without leaving a tag / PR behind.
    if args.release_type == "patch" and not args.only_docker:
        step(
            name="Download All Release Artifacts",
            command=[
                f"python3 ./ci/jobs/scripts/create_release.py --download-packages"
                f" {dry_run_flag}".strip()
            ],
            workdir=REPO_PATH,
        )

    if create_new_release:
        step(
            name="Push Git Tag for the Release",
            command=[
                f"python3 ./ci/jobs/scripts/create_release.py --push-release-tag"
                f" {dry_run_flag}".strip()
            ],
            workdir=REPO_PATH,
        )

    if args.release_type == "new" and create_new_release:
        step(
            name="Push New Release Branch",
            command=[
                f"python3 ./ci/jobs/scripts/create_release.py --push-new-release-branch"
                f" {dry_run_flag}".strip()
            ],
            workdir=REPO_PATH,
        )

    # For a "new" release the version bump also opens the master bump PR that
    # the merge_prs step merges below, so it must run here, before that merge. For a
    # "patch" release the bump is only a direct push of the branch version file
    # and nothing downstream depends on it; it is deferred to the very end of the
    # run (after the merge_prs step) so that a rerun after any failure between the tag
    # push and the end always sees an un-bumped branch. prepare then reads the
    # branch tip as the just-released version, recovers the existing release, and
    # never refuses a rerun as "out-of-order" or mints a release below the tip —
    # all without scanning git tags. See the deferred step near the end of main.
    if args.release_type == "new" and release_pr_absent:
        step(
            name="Bump CH Version and Update Contributors' List",
            command=[
                f"python3 ./ci/jobs/scripts/create_release.py --create-bump-version-pr"
                f" {dry_run_flag}".strip()
            ],
            workdir=REPO_PATH,
        )

    if ok and args.release_type == "patch" and release_pr_absent:
        with open(RELEASE_INFO_FILE) as f:
            release_tag = json.load(f)["release_tag"]
        uid = os.getuid()
        gid = os.getgid()
        step(
            name="Bump Docker Versions, Changelog, Security",
            command=[
                "echo 'List versions'",
                "./utils/list-versions/list-versions.sh"
                " > ./utils/list-versions/version_date.tsv",
                "echo 'Update docker version'",
                "./utils/list-versions/update-docker-version.sh",
                "echo 'Generate ChangeLog'",
                "docker pull clickhouse/style-test:latest",
                # changelog.py runs inside the container, which cannot see the
                # host gh session, so pass the robot token in via `-e GH_TOKEN`
                # (inherited from the job-wide export) and `--gh-user-or-token`.
                # The command string carries `$GH_TOKEN`, not its value, so
                # verbose logging never prints the token.
                f"CI=1 docker run -u {uid}:{gid} -e PYTHONUNBUFFERED=1 -e CI=1"
                f" -e GH_TOKEN --network=host --volume='{REPO_PATH}:/wd' --workdir=/wd"
                f" clickhouse/style-test:latest"
                f" ./tests/ci/changelog.py -v --debug-helpers"
                f' --gh-user-or-token "$GH_TOKEN"'
                f" --jobs=5"
                f" --output=./docs/changelogs/{release_tag}.md {release_tag}",
                f"git add ./docs/changelogs/{release_tag}.md",
                "echo 'Generate Security'",
                "python3 ./utils/security-generator/generate_security.py"
                " > SECURITY.md",
                "git diff HEAD",
            ],
            workdir=REPO_PATH,
        )

    if ok and args.release_type == "patch" and not args.dry_run and release_pr_absent:
        with open(RELEASE_INFO_FILE) as f:
            release_tag = json.load(f)["release_tag"]

        def create_changelog_pr():
            pr_branch = f"auto/{release_tag}"
            commit_msg = f"Update version_date.tsv and changelogs after {release_tag}"
            pr_title = f"Update version_date.tsv and changelog after {release_tag}"
            pr_body = (
                f"Update version_date.tsv and changelogs after {release_tag}\n"
                "### Changelog category (leave one):\n"
                "- Not for changelog (changelog entry is not required)"
            )

            Shell.check(
                "git config user.email robot-clickhouse@users.noreply.github.com",
                strict=True,
            )
            Shell.check("git config user.name robot-clickhouse", strict=True)
            # The PR must contain ONLY the generated release artifacts, on a clean
            # master base - never the unrelated commits HEAD carries when the
            # release runs from a feature branch, nor any file `git add -A` would
            # sweep in. Capture whatever the generation steps above changed, but
            # scope the scan to exactly their output paths so a stray file left
            # elsewhere on a reused runner cannot leak in; then hard-reset onto
            # origin/master (-f, so the switch can't abort on "local changes would
            # be overwritten"), restore those paths, and stage only them. -B so a
            # rerun re-creates the branch instead of failing on "already exists".
            # Collect the paths as clean, one-per-line names (NOT via
            # `git status --porcelain` slicing: Shell.get_output strips the
            # output, which eats the leading space of a worktree-modified line and
            # would drop a char off the first path). Tracked changes vs HEAD +
            # any untracked new files, scoped to the artifact paths.
            # The exact files the generation steps touch. update-docker-version.sh
            # bumps `ARG VERSION` in these Dockerfiles (keeper's Dockerfile.alpine
            # / Dockerfile.ubuntu are symlinks to keeper/Dockerfile, so the edit
            # lands on keeper/Dockerfile itself). Listed explicitly rather than
            # globbed so nothing unexpected is ever swept in.
            pathspec = " ".join(
                [
                    "utils/list-versions/version_date.tsv",
                    "docs/changelogs/" + shlex.quote(release_tag) + ".md",
                    "SECURITY.md",
                    "docker/keeper/Dockerfile",
                    "docker/keeper/Dockerfile.distroless",
                    "docker/server/Dockerfile.alpine",
                    "docker/server/Dockerfile.distroless",
                    "docker/server/Dockerfile.ubuntu",
                ]
            )
            changed = Shell.get_output(
                f"git diff --name-only HEAD -- {pathspec}", strict=True
            )
            untracked = Shell.get_output(
                f"git ls-files --others --exclude-standard -- {pathspec}", strict=True
            )
            artifact_files = sorted(
                {f for f in changed.splitlines() + untracked.splitlines() if f.strip()}
            )
            backup_dir = tempfile.mkdtemp(prefix="changelog-artifacts-")
            for f in artifact_files:
                dst = os.path.join(backup_dir, f)
                os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)
                shutil.copy2(f, dst)
            Shell.check(f"git checkout -f -B {pr_branch} origin/master", strict=True)
            for f in artifact_files:
                os.makedirs(os.path.dirname(f) or ".", exist_ok=True)
                shutil.copy2(os.path.join(backup_dir, f), f)
            shutil.rmtree(backup_dir, ignore_errors=True)
            if artifact_files:
                Shell.check(
                    "git add -- "
                    + " ".join(shlex.quote(f) for f in artifact_files),
                    strict=True,
                )
            # If the changelog PR was already merged on a previous run, master
            # (and this branch, freshly checked out from it) already contain the
            # generated files, so there is nothing to commit — `git commit`
            # would fail with "nothing to commit". Only commit and push when
            # there are staged changes; the already-merged PR is then picked up
            # by the existing-PR lookup below, which skips `gh pr create`.
            if Shell.check("git diff --cached --quiet"):
                print(
                    "No changelog/version changes to commit — already up to date,"
                    " skipping commit/push"
                )
            else:
                Shell.check(
                    f"git commit -m {shlex.quote(commit_msg)}",
                    strict=True,
                )
                # Retry the spurious "Unable to determine if workflow can be
                # created or updated due to timeout; `workflows` scope may be
                # required" rejection that GitHub's push-time workflow-file check
                # throws on a repo this size (the same transient push_release_tag
                # retries past). GH_TOKEN is the robot PAT, which carries the
                # workflow scope, so the scope itself is not the problem.
                Git.push(
                    "ClickHouse/ClickHouse",
                    f"{pr_branch}:{pr_branch}",
                    force=True,
                    strict=True,
                    retries=3,
                )

            with tempfile.NamedTemporaryFile(
                mode="w", delete=False, suffix=".txt", encoding="utf-8"
            ) as body_file:
                body_file.write(pr_body)
                body_file_path = body_file.name

            try:
                # On a rerun after a partial failure the PR may already exist for
                # this branch (the branch is force-pushed above); `gh pr create`
                # would then fail with "already exists". Only treat an OPEN or
                # MERGED PR as reusable — a PR closed without merge must be
                # recreated, otherwise the downstream merge_prs step (which looks up
                # open/merged PRs) would find nothing and fail after publication.
                existing_pr = GH.get_pr_url_by_branch(
                    branch=pr_branch, repo="ClickHouse/ClickHouse"
                )
                if existing_pr:
                    print(f"ChangeLog PR already exists [{existing_pr}] — skipping create")
                else:
                    cmd = (
                        f"gh pr create --base master --head {shlex.quote(pr_branch)}"
                        f" --title {shlex.quote(pr_title)}"
                        f" --body-file {body_file_path}"
                        f" --label 'do not test'"
                        + (
                            f" --assignee {shlex.quote(args.assignee)}"
                            if args.assignee
                            else ""
                        )
                    )
                    assert GH.do_command_with_retries(cmd), "Failed to create PR"
            finally:
                os.unlink(body_file_path)

        step(
            name="Create ChangeLog PR",
            command=create_changelog_pr,
            workdir=REPO_PATH,
        )

    if (
        args.release_type == "patch"
        and not args.only_repo
        and not args.only_docker
    ):
        # Restore the working tree after the changelog/version-bump steps, which
        # dirty it. A no-op on recovery / out-of-order runs (they skip the
        # changelog steps); the always-run "Checkout Back" below is the safety net
        # if anything between here and there leaves the tree dirty.
        step(
            name="Restore Git State",
            command=[
                "git reset --hard HEAD",
                f"git checkout {original_branch}",
            ],
            workdir=REPO_PATH,
        )

        step(
            name="Create GH Release",
            command=[
                f"python3 ./ci/jobs/scripts/create_release.py --create-gh-release"
                f" {dry_run_flag}".strip()
            ],
            workdir=REPO_PATH,
        )

    if args.release_type == "patch" and not args.only_docker:
        for name, flag in (
            ("Export TGZ Packages", "--export-tgz"),
            ("Test TGZ Packages", "--test-tgz"),
            ("Export RPM Packages", "--export-rpm"),
            ("Test RPM Packages", "--test-rpm"),
            ("Export Debian Packages", "--export-debian"),
            ("Test Debian Packages", "--test-debian"),
        ):
            step(
                name=name,
                command=[
                    f"python3 ./ci/jobs/scripts/artifactory.py {flag}"
                    f" {dry_run_flag}".strip()
                ],
                workdir=REPO_PATH,
            )

    if ok and args.release_type == "patch" and not args.dry_run:
        with open(RELEASE_INFO_FILE) as f:
            release_info = json.load(f)
        release_tag = release_info["release_tag"]
        # is_branch_release: this release is the latest on its branch → publish
        # the floating minor/major tags. is_latest: its branch is the latest
        # release branch → additionally publish `latest`. These decide the
        # floating tags by whether the release is current, so recovery of the
        # current release re-applies them while recovery of a superseded one
        # only re-publishes its exact version tag.
        is_branch_release = release_info["is_branch_release"]
        is_latest = release_info["latest"]

        def _make_docker_build(
            image: str,
            build_configs: List[Tuple[str, str, str]],
        ):
            def build():
                Shell.check(f"git checkout {release_tag}", strict=True)

                m = re.match(r"^v(\d+\.\d+\.\d+\.\d+)", release_tag)
                assert m, f"Cannot parse version from tag {release_tag}"
                version_string = m.group(1)
                parts = version_string.split(".")
                version_minor = ".".join(parts[:3])
                version_major = ".".join(parts[:2])

                for variant, dockerfile, context in build_configs:
                    # Older release tags may not ship every Dockerfile (e.g.
                    # distroless was added later); skip variants whose Dockerfile
                    # is absent at this tag, matching the legacy workflow.
                    if not os.path.isfile(dockerfile):
                        print(f"Skipping {variant}: {dockerfile} not found at this tag")
                        continue
                    version_suffix = "" if variant == "ubuntu" else f"-{variant}"
                    label_version = f"{version_string}{version_suffix}"
                    # Always publish the exact version tag.
                    tags = [f"--tag={image}:{version_string}{version_suffix}"]
                    # Floating minor/major tags must point at the latest release
                    # on the branch, so move them only when this release is that
                    # latest one (is_branch_release) — true for a normal release
                    # and for recovery of the current release, false for recovery
                    # of a superseded tag (which would otherwise move them back to
                    # an older image).
                    if is_branch_release:
                        tags += [
                            f"--tag={image}:{version_minor}{version_suffix}",
                            f"--tag={image}:{version_major}{version_suffix}",
                        ]
                        # `latest` additionally requires the branch to be the
                        # latest release branch.
                        if is_latest:
                            tags.append(f"--tag={image}:latest{version_suffix}")

                    # The multi-arch buildx log is large; praktika captures and
                    # truncates the tail of a step's output, which hides the
                    # actual build error. Redirect the full log to a file and,
                    # on failure, print just its tail so the real error survives.
                    image_slug = image.replace("/", "_")
                    build_log = f"/tmp/docker_build_{image_slug}_{variant}.log"
                    # The distroless image is a multi-stage Dockerfile; build the
                    # production target explicitly so the published image is the
                    # minimal runtime stage, not an earlier build stage.
                    target_arg = (
                        " --target=production" if variant == "distroless" else ""
                    )
                    Shell.check(
                        f"docker buildx build"
                        f" --platform=linux/amd64,linux/arm64"
                        f" --provenance=true"
                        f" --sbom=true"
                        f" --output=type=registry"
                        f"{target_arg}"
                        f" --label=com.clickhouse.build.version={label_version}"
                        f" {' '.join(tags)}"
                        f" --build-arg=VERSION={version_string}"
                        f" --progress=plain"
                        f" --file={dockerfile}"
                        f" {context}"
                        f" > {build_log} 2>&1"
                        f" || (echo '=== docker buildx build failed for"
                        f" {image}:{label_version}; tail of {build_log}: ==='"
                        f" >&2; tail -n 200 {build_log} >&2; exit 1)",
                        strict=True,
                    )

                Shell.check("git checkout -", strict=True)

            return build

        step(
            name="Set up Docker buildx (multi-arch)",
            command=[
                # The ephemeral runner is not pre-provisioned for multi-arch
                # docker builds (the legacy dedicated runner was). The release
                # images are built for both linux/amd64 and linux/arm64 in one
                # buildx invocation, so each run must register QEMU/binfmt (to
                # emulate the non-native arch — the runner is x86_64, so amd64
                # is native and arm64 is emulated) and create a
                # `docker-container` builder (the default `docker` driver cannot
                # produce a multi-platform image or push to a registry).
                f"docker run --privileged --rm {_BINFMT_IMAGE} --install all",
                # Create the builder with host networking. The default
                # docker-container sandbox network on the ephemeral runner has
                # no working egress for RUN steps (busybox wget in the alpine
                # image hits "Network unreachable" reaching
                # packages.clickhouse.com — an IPv6 address without a route),
                # while the host's IPv4 egress works. `network=host` makes RUN
                # steps use the host network stack. Use a dedicated builder name
                # (not the default `mybuilder`) and recreate it unconditionally,
                # so we never clobber a pre-provisioned shared builder and always
                # get one with this network option.
                "docker buildx rm release-builder >/dev/null 2>&1 || true",
                "docker buildx create --name release-builder --driver docker-container"
                " --driver-opt network=host --use",
                "docker buildx inspect --bootstrap",
            ],
            workdir=REPO_PATH,
        )

        step(
            name="Docker clickhouse/clickhouse-server Building",
            command=_make_docker_build(
                image="clickhouse/clickhouse-server",
                build_configs=[
                    (
                        "ubuntu",
                        "docker/server/Dockerfile.ubuntu",
                        "docker/server",
                    ),
                    (
                        "alpine",
                        "docker/server/Dockerfile.alpine",
                        "docker/server",
                    ),
                    (
                        "distroless",
                        "docker/server/Dockerfile.distroless",
                        "docker/server",
                    ),
                ],
            ),
            workdir=REPO_PATH,
        )

        step(
            name="Docker clickhouse/clickhouse-keeper Building",
            command=_make_docker_build(
                image="clickhouse/clickhouse-keeper",
                build_configs=[
                    (
                        "ubuntu",
                        "docker/keeper/Dockerfile.ubuntu",
                        "docker/keeper",
                    ),
                    (
                        "alpine",
                        "docker/keeper/Dockerfile.alpine",
                        "docker/keeper",
                    ),
                    (
                        "distroless",
                        "docker/keeper/Dockerfile.distroless",
                        "docker/keeper",
                    ),
                ],
            ),
            workdir=REPO_PATH,
        )

    # Always restore git state — equivalent to `if: ${{ !cancelled() }}`, so it
    # must run even after a failure (hence Result.from_commands_run, not step()
    # which skips when ok is already False). But a failed restore must still
    # block the release mutation below (the merge_prs step), so fold its result into ok.
    results.append(
        Result.from_commands_run(
            name="Checkout Back",
            command=[f"git checkout {original_branch}"],
            workdir=REPO_PATH,
        )
    )
    if results[-1].status != Result.Status.OK:
        ok = False

    # Deferred patch version bump. Bumping the branch version file here (rather
    # than right after the tag push) keeps the branch tip equal to the released
    # commit for the whole publish phase, so any rerun in that window sees an
    # un-bumped branch and prepare recovers the existing release instead of
    # refusing it or minting a below-tip release. `step` skips it when a prior
    # step already failed, so a failed publish leaves the branch un-bumped and
    # recoverable. ("new" bumps earlier, above, because the merge step below
    # merges the master bump PR it opens.)
    if create_new_release and args.release_type == "patch":
        step(
            name="Bump CH Version and Update Contributors' List",
            command=[
                f"python3 ./ci/jobs/scripts/create_release.py --create-bump-version-pr"
                f" {dry_run_flag}".strip()
            ],
            workdir=REPO_PATH,
        )

    # Enqueue the release PR - the LAST release action, so its `CH Inc sync`
    # required check gets the maximum time (the whole publish above) to complete
    # before we enqueue. Only when every preceding step succeeded (step() skips
    # when ok is already False). Merge the PR created earlier (or left open by a
    # failed prior run); skipped only when it is already merged (the "merged ->
    # continue" branch). merge_prs looks the PR up by branch and enqueues it
    # (best-effort); a no-op if the lookup finds nothing.
    def merge_created_prs():
        # Imported lazily so the module-level boto3 dependency of create_release
        # is only needed on the release machine, not at praktika config time.
        from ci.jobs.scripts.create_release import (
            ReleaseContextManager,
            ReleaseProgress,
        )

        with ReleaseContextManager(
            release_progress=ReleaseProgress.MERGE_CREATED_PRS
        ) as release_info:
            release_info.update_release_info(dry_run=args.dry_run)
            release_info.merge_prs(dry_run=args.dry_run)

    if release_pr_needs_merge:
        step(
            name="Update Release Info and Merge Created PRs",
            command=merge_created_prs,
            workdir=REPO_PATH,
        )

    # Post the final release status — but only when "Prepare Release Info" ran
    # this attempt and produced RELEASE_INFO_FILE. If an early setup step failed
    # before prepare, the file is absent (cleared at the top of main), so
    # --post-status would raise FileNotFoundError trying to read it; skip it and
    # let the aggregated job Result (praktika Slack feed) report the failing
    # setup step instead.
    if os.path.exists(RELEASE_INFO_FILE):
        results.append(
            Result.from_commands_run(
                name="Post Slack Message",
                command=[
                    f"python3 ./ci/jobs/scripts/create_release.py --post-status"
                    f" {dry_run_flag}".strip()
                ],
                workdir=REPO_PATH,
            )
        )

    # Always remove the publishing credentials and the signing-key home so they
    # do not persist for a later job on a reused self-hosted runner.
    def cleanup_credentials():
        # Unmount the geesefs FUSE mount of the production `packages` bucket
        # first. ci/jobs/scripts/artifactory.py mounts it at ~/mountpoint and its
        # teardown is not in a finally block, so an export step that fails
        # before teardown leaves the bucket mounted; on a reused runner the next
        # job could still read or mutate it even after the R2 auth files below
        # are deleted. Mirror artifactory.py's `umount ~/mountpoint`, and let a
        # failed unmount fail this cleanup step (strict) rather than silently
        # leaving the bucket accessible.
        mount_point = os.path.expanduser("~/mountpoint")
        if os.path.ismount(mount_point):
            Shell.check(f"umount {mount_point}", strict=True, verbose=True)
        for name in ("~/.r2_auth", "~/.r2_auth_test"):
            path = os.path.expanduser(name)
            if os.path.exists(path):
                os.remove(path)
        # The env script holds the write-scoped release PAT
        # (ROBOT_CLICKHOUSE_COMMIT_TOKEN); remove it so the token does not
        # persist in ci/tmp for a later job on a reused runner.
        Path(REPO_PATH, "ci/tmp/praktika_setup_env.sh").unlink(missing_ok=True)
        if gnupg_home:
            shutil.rmtree(gnupg_home, ignore_errors=True)
        if docker_config:
            shutil.rmtree(docker_config, ignore_errors=True)

    results.append(
        Result.from_commands_run(
            name="Clean Up Credentials",
            command=cleanup_credentials,
            workdir=REPO_PATH,
        )
    )

    log_files = [
        p
        for p in [
            "/tmp/reprepro.log",
            "/tmp/createrepo_c.log",
            os.path.expanduser("~/fuse_mount.log"),
            RELEASE_INFO_FILE,
        ]
        if os.path.isfile(p)
    ]
    Result.create_from(results=results, stopwatch=stopwatch, files=log_files).complete_job()


if __name__ == "__main__":
    main()
