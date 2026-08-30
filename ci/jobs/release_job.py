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
        "--skip-repo",
        action="store_true",
        help="Skip repo updates (package export/test)",
    )
    parser.add_argument(
        "--skip-docker",
        action="store_true",
        help="Skip docker image builds",
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
    if not args.skip_repo:
        args.skip_repo = _wi("skip-repo").lower() == "true"
    if not args.skip_docker:
        args.skip_docker = _wi("skip-docker").lower() == "true"
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
    # stop the run before partial publication. Gated on the docker phase running
    # this attempt (patch, not dry-run, docker not skipped).
    if args.release_type == "patch" and not args.dry_run and not args.skip_docker:

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

    if args.release_type == "patch" and not args.skip_repo:
        # Skipped on dry-run (local convenience).
        if not args.dry_run:
            step(
                # The tools are baked into the release-maker image; fail closed rather than fetch third-party code on a credentialed host.
                name="Verify release tools",
                command=[
                    "geesefs --version"
                    " && createrepo_c --version"
                    " && reprepro --version 2>&1 | grep -qE 'reprepro version 5\\.([4-9]|[1-9][0-9])'"
                    " || { echo 'ERROR: geesefs, createrepo_c and reprepro 5.4+ must be"
                    " installed for a release' >&2; exit 1; }"
                ],
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
    # recovery (skip-repo/skip-docker) or an out-of-order full run skips them
    # without erroring and just re-exports repos / rebuilds docker.
    # If a prior step failed (ok is False) the prepared flags are unread; default both landmarks to "already done" so no creation step fires.
    is_tag_pushed = True
    is_bump_landed = True
    if ok:
        with open(RELEASE_INFO_FILE) as f:
            _prepared = json.load(f)
        is_tag_pushed = _prepared["is_tag_pushed"]
        is_bump_landed = _prepared["is_bump_landed"]

    # skip-repo / skip-docker mark a partial run that only re-publishes artifacts
    # for an already-created release (repo/Docker recovery). If the ref resolves
    # to a new release, they would otherwise fall through to the creation steps
    # below (push tag, bump version, PRs) and produce a partial new release, so
    # reject that misuse and require the release tag instead.
    if ok and not is_tag_pushed and (args.skip_repo or args.skip_docker):

        def _require_recovery_ref():
            raise RuntimeError(
                "skip-repo/skip-docker re-publish an existing release and must be "
                "run against its release tag (recovery); the given ref resolves to "
                "a new release. Pass the release tag as the ref."
            )

        step(name="Validate Recovery Ref", command=_require_recovery_ref)

    # patch pushes its changelog to master; detect whether it is already there so a rerun is idempotent. The "new" bump self-checks the master version instead.
    changelog_absent = False
    if args.release_type == "patch":
        if args.dry_run:
            changelog_absent = not is_tag_pushed
        elif ok:
            with open(RELEASE_INFO_FILE) as f:
                _info = json.load(f)
            changelog_path = f"docs/changelogs/{_info['release_tag']}.md"
            on_master = bool(
                Shell.get_output(
                    f"git ls-tree --name-only origin/master -- {shlex.quote(changelog_path)}"
                ).strip()
            )
            changelog_absent = not on_master
            print(
                f"ChangeLog [{changelog_path}] on master: "
                + ("yes — skipping" if on_master else "no — will push")
            )

    # Fail-fast: verify the release packages exist (this downloads them) before
    # pushing the tag or opening the changelog PR, so a missing-artifacts run
    # aborts without leaving a tag / PR behind.
    if args.release_type == "patch" and not args.skip_repo:
        step(
            name="Download All Release Artifacts",
            command=[
                f"python3 ./ci/jobs/scripts/create_release.py --download-packages"
                f" {dry_run_flag}".strip()
            ],
            workdir=REPO_PATH,
        )

    if not is_tag_pushed:
        step(
            name="Push Git Tag for the Release",
            command=[
                f"python3 ./ci/jobs/scripts/create_release.py --push-release-tag"
                f" {dry_run_flag}".strip()
            ],
            workdir=REPO_PATH,
        )

    if args.release_type == "new" and not is_tag_pushed:
        step(
            name="Push New Release Branch",
            command=[
                f"python3 ./ci/jobs/scripts/create_release.py --push-new-release-branch"
                f" {dry_run_flag}".strip()
            ],
            workdir=REPO_PATH,
        )

    # "new" bumps master here (idempotent — it self-checks master's version). "patch" defers its branch bump to the end of the run for recovery-safety; see the deferred step near the end of main.
    if args.release_type == "new":
        step(
            name="Bump CH Version and Update Contributors' List",
            command=[
                f"python3 ./ci/jobs/scripts/create_release.py --create-bump-version-pr"
                f" {dry_run_flag}".strip()
            ],
            workdir=REPO_PATH,
        )

    if ok and args.release_type == "patch" and changelog_absent:
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

    if ok and args.release_type == "patch" and not args.dry_run and changelog_absent:
        with open(RELEASE_INFO_FILE) as f:
            release_tag = json.load(f)["release_tag"]

        def push_changelog_to_master():
            commit_msg = f"Update version_date.tsv and changelogs after {release_tag}"
            Shell.check(
                "git config user.email robot-clickhouse@users.noreply.github.com"
                " && git config user.name robot-clickhouse",
                strict=True,
            )
            # The exact files the generation step touches; scanned vs HEAD + untracked.
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
            assert artifact_files, "no changelog artifacts were generated"
            # Back up the generated files; the checkout below discards the worktree.
            backup_dir = tempfile.mkdtemp(prefix="changelog-artifacts-")
            for f in artifact_files:
                dst = os.path.join(backup_dir, f)
                os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)
                shutil.copy2(f, dst)
            try:
                Shell.check(
                    "git fetch --quiet origin master && git checkout -f FETCH_HEAD",
                    strict=True,
                )
                for f in artifact_files:
                    os.makedirs(os.path.dirname(f) or ".", exist_ok=True)
                    shutil.copy2(os.path.join(backup_dir, f), f)
                Shell.check(
                    "git add -- " + " ".join(shlex.quote(f) for f in artifact_files),
                    strict=True,
                )
                # Already on master (rerun) — nothing to push.
                if Shell.check("git diff --cached --quiet"):
                    print("ChangeLog already on master — nothing to push")
                    return
                Shell.check(f"git commit -m {shlex.quote(commit_msg)}", strict=True)
                Git.push(
                    "ClickHouse/ClickHouse",
                    "HEAD:refs/heads/master",
                    strict=True,
                    retries=3,
                    rebase_retries=5,
                )
            finally:
                shutil.rmtree(backup_dir, ignore_errors=True)

        step(
            name="Push ChangeLog to master",
            command=push_changelog_to_master,
            workdir=REPO_PATH,
        )

    if args.release_type == "patch" and not args.skip_repo:
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

    if args.release_type == "patch" and not args.skip_repo:
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

    if (
        ok
        and args.release_type == "patch"
        and not args.dry_run
        and not args.skip_docker
    ):
        with open(RELEASE_INFO_FILE) as f:
            release_info = json.load(f)
        release_tag = release_info["release_tag"]
        # Branch head (bump not landed) → move floating minor/major tags; is_latest also moves `latest`. A later recovery (bump landed) leaves them as they are.
        is_bump_landed = release_info["is_bump_landed"]
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
                    # Move the floating minor/major tags only for the branch head (bump not landed), so a later recovery does not point them back at an older image.
                    if not is_bump_landed:
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
                        # Pinned scanner: the floating stable-1 tag can move to a
                        # version whose scan exceeds the runner's memory.
                        f" --attest=type=sbom,generator=docker/buildkit-syft-scanner:1.11"
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

    # Always restore git state (Result.from_commands_run, not step(), so it runs after a failure too); a failed restore folds into ok to block the deferred bump below.
    results.append(
        Result.from_commands_run(
            name="Checkout Back",
            command=[f"git checkout {original_branch}"],
            workdir=REPO_PATH,
        )
    )
    if results[-1].status != Result.Status.OK:
        ok = False

    # Deferred to the end so a rerun before it sees an un-bumped branch and prepare recovers the release; `not is_bump_landed` completes an unfinished bump once and never rewrites a landed one.
    if not is_bump_landed and args.release_type == "patch":
        step(
            name="Bump CH Version and Update Contributors' List",
            command=[
                f"python3 ./ci/jobs/scripts/create_release.py --create-bump-version-pr"
                f" {dry_run_flag}".strip()
            ],
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
