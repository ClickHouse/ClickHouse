import argparse
import json
import os
import re
import shlex
import shutil
import tempfile
from typing import List, Tuple

from ci.praktika.gh import GH
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

    dry_run_flag = "--dry-run" if args.dry_run else ""
    original_branch = Shell.get_output("git rev-parse --abbrev-ref HEAD", strict=True)
    # Per-run GNUPGHOME for the signing key (set when the GPG import step runs);
    # removed by the cleanup step so the private key never persists on a reused
    # runner.
    gnupg_home = None

    # Export the robot token once for the whole job (the legacy workflow set it
    # as a job-level `env: GH_TOKEN`). Commands then reference `$GH_TOKEN` instead
    # of interpolating the secret value, so praktika's verbose command logging
    # (Shell.run prints every command) never writes the token to the job log.
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
        name="Fetch Full Repository",
        command=[
            "git fetch --unshallow --no-recurse-submodules origin ||:",
            # actions/checkout configures origin to fetch only the workflow ref,
            # but prepare reads origin/<release_branch> and a commit_sha that an
            # auto_releases run may pass from a different branch. Fetch all heads
            # and tags so those refs are always present.
            "git fetch --no-recurse-submodules origin '+refs/heads/*:refs/remotes/origin/*'",
            "git fetch --tags --no-recurse-submodules origin",
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
                "command -v createrepo_c || sudo apt-get install -y createrepo-c ||:",
                # reprepro 5.4.4+ is required for the 'Limit' field in distributions config.
                # Ubuntu Jammy only has 5.3.0, so build from source if needed.
                "reprepro --version 2>&1 | grep -qE '5\\.[4-9]' || ("
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
                    "command -v createrepo_c >/dev/null && command -v reprepro >/dev/null"
                    " || { echo 'ERROR: createrepo_c and reprepro must be installed"
                    " for a release' >&2; exit 1; }"
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

    # only-repo / only-docker are recovery runs against an already-released tag:
    # origin/<release_branch> has been version-bumped since, so prepare's
    # out-of-order guard must be relaxed (as the legacy workflow did).
    skip_out_of_order_check_flag = (
        "--skip-out-of-order-check" if (args.only_repo or args.only_docker) else ""
    )
    step(
        name="Prepare Release Info",
        command=[
            f"python3 ./ci/jobs/create_release.py --prepare-release-info"
            f" --ref {shlex.quote(args.ref)} --release-type {args.release_type}"
            f" {skip_out_of_order_check_flag} {dry_run_flag}".strip()
        ],
        workdir=REPO_PATH,
    )

    if args.release_type == "patch" and not args.only_docker:
        step(
            name="Download All Release Artifacts",
            command=[
                f"python3 ./ci/jobs/create_release.py --download-packages"
                f" {dry_run_flag}".strip()
            ],
            workdir=REPO_PATH,
        )

    if not args.only_repo and not args.only_docker:
        step(
            name="Push Git Tag for the Release",
            command=[
                f"python3 ./ci/jobs/create_release.py --push-release-tag"
                f" {dry_run_flag}".strip()
            ],
            workdir=REPO_PATH,
        )

    if args.release_type == "new" and not args.only_repo and not args.only_docker:
        step(
            name="Push New Release Branch",
            command=[
                f"python3 ./ci/jobs/create_release.py --push-new-release-branch"
                f" {dry_run_flag}".strip()
            ],
            workdir=REPO_PATH,
        )

    if not args.only_repo and not args.only_docker:
        step(
            name="Bump CH Version and Update Contributors' List",
            command=[
                f"python3 ./ci/jobs/create_release.py --create-bump-version-pr"
                f" {dry_run_flag}".strip()
            ],
            workdir=REPO_PATH,
        )

    if (
        ok
        and args.release_type == "patch"
        and not args.only_repo
        and not args.only_docker
    ):
        with open(RELEASE_INFO_FILE) as f:
            release_tag = json.load(f)["release_tag"]
        uid = os.getuid()
        gid = os.getgid()
        step(
            name="Bump Docker Versions, Changelog, Security",
            command=[
                "python3 ./ci/jobs/create_release.py --set-progress-started"
                " --progress 'update changelog, docker version, security'",
                "echo 'List versions'",
                "./utils/list-versions/list-versions.sh"
                " > ./utils/list-versions/version_date.tsv",
                "echo 'Update docker version'",
                "./utils/list-versions/update-docker-version.sh",
                "echo 'Generate ChangeLog'",
                "docker pull clickhouse/style-test:latest",
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

    if (
        ok
        and args.release_type == "patch"
        and not args.dry_run
        and not args.only_repo
        and not args.only_docker
    ):
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
            # -B so a rerun after a partial failure re-creates the branch instead
            # of failing on "branch already exists".
            Shell.check(f"git checkout -B {pr_branch}", strict=True)
            Shell.check("git add -A", strict=True)
            Shell.check(
                f"git commit -m {shlex.quote(commit_msg)}",
                strict=True,
            )
            Shell.check(
                f"git push --force https://x-access-token:$GH_TOKEN@github.com/ClickHouse/ClickHouse.git {pr_branch}",
                strict=True,
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
                # recreated, otherwise the downstream --merge-prs (which looks up
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
        step(
            name="Complete Previous Steps and Restore Git State",
            command=[
                "git reset --hard HEAD",
                f"git checkout {original_branch}",
                "python3 ./ci/jobs/create_release.py --set-progress-completed",
            ],
            workdir=REPO_PATH,
        )

        step(
            name="Create GH Release",
            command=[
                f"python3 ./ci/jobs/create_release.py --create-gh-release"
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
                    f"python3 ./tests/ci/artifactory.py {flag}"
                    f" {dry_run_flag}".strip()
                ],
                workdir=REPO_PATH,
            )

    if ok and args.release_type == "patch" and not args.dry_run:
        with open(RELEASE_INFO_FILE) as f:
            release_info = json.load(f)
        release_tag = release_info["release_tag"]
        is_latest = release_info["latest"]

        def _make_docker_build(
            image: str,
            progress: str,
            build_configs: List[Tuple[str, str, str]],
        ):
            def build():
                Shell.check(
                    f"python3 ./ci/jobs/create_release.py --set-progress-started"
                    f" --progress {shlex.quote(progress)}",
                    strict=True,
                )
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
                    # Floating tags (minor/major/latest) must move only on a
                    # normal release. A recovery run (only-repo / only-docker)
                    # rebuilds an already-released tag that may no longer be the
                    # current release on its branch, so moving the floating tags
                    # would point them back to an older image. Skip them.
                    if not (args.only_repo or args.only_docker):
                        tags += [
                            f"--tag={image}:{version_minor}{version_suffix}",
                            f"--tag={image}:{version_major}{version_suffix}",
                        ]
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
                Shell.check(
                    "python3 ./ci/jobs/create_release.py --set-progress-completed",
                    strict=True,
                )

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
                progress="docker server release",
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
                progress="docker keeper release",
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

    # Always restore git state — equivalent to `if: ${{ !cancelled() }}`.
    results.append(
        Result.from_commands_run(
            name="Checkout Back",
            command=[f"git checkout {original_branch}"],
            workdir=REPO_PATH,
        )
    )

    # Merging the created PRs and marking the release "completed" are release
    # mutations that must only happen when every preceding step succeeded. Use
    # step(), which skips when ok is already False and folds its own result back
    # into ok — so if --merge-prs fails, the release is NOT marked completed and
    # the Slack post below reports the actual failing step.
    step(
        name="Update Release Info and Merge Created PRs",
        command=[
            f"python3 ./ci/jobs/create_release.py --merge-prs"
            f" {dry_run_flag}".strip()
        ],
        workdir=REPO_PATH,
    )

    step(
        name="Set Release Progress to Completed",
        command=[
            "python3 ./ci/jobs/create_release.py --set-progress-started"
            " --progress completed",
            "python3 ./ci/jobs/create_release.py --set-progress-completed",
        ],
        workdir=REPO_PATH,
    )

    # Always post the final status (the completed release, or the failing step).
    results.append(
        Result.from_commands_run(
            name="Post Slack Message",
            command=[
                f"python3 ./ci/jobs/create_release.py --post-status"
                f" {dry_run_flag}".strip()
            ],
            workdir=REPO_PATH,
        )
    )

    # Always remove the publishing credentials and the signing-key home so they
    # do not persist for a later job on a reused self-hosted runner.
    def cleanup_credentials():
        for name in ("~/.r2_auth", "~/.r2_auth_test"):
            path = os.path.expanduser(name)
            if os.path.exists(path):
                os.remove(path)
        if gnupg_home:
            shutil.rmtree(gnupg_home, ignore_errors=True)

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
