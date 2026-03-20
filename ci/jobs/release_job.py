import argparse
import json
import os
import re
import shlex
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

_GEESEFS_VERSION = "v0.43.5"

_R2_AUTH_TEST_SECRET = Secret.Config(
    name="/release/r2-auth-test",
    type=Secret.Type.AWS_SSM_PARAMETER,
)
_R2_AUTH_PROD_SECRET = Secret.Config(
    name="/release/r2-auth",
    type=Secret.Type.AWS_SSM_PARAMETER,
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

    # When CLI args are absent, fall back to workflow dispatch inputs (CI runs).
    def _wi(name: str) -> str:
        return Info.get_workflow_input_value(name) or ""

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

    results = []
    ok = True

    def step(**kwargs):
        nonlocal ok
        if not ok:
            return
        results.append(Result.from_commands_run(**kwargs))
        if results[-1].status != Result.Status.SUCCESS:
            ok = False

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
                "command -v reprepro || sudo apt-get install -y reprepro ||:",
            ],
            workdir=REPO_PATH,
        )

        def write_r2_auth():
            r2_auth_test = _R2_AUTH_TEST_SECRET.get_value()
            with open(os.path.expanduser("~/.r2_auth_test"), "w") as f:
                f.write(r2_auth_test)
            if not args.dry_run:
                r2_auth_prod = _R2_AUTH_PROD_SECRET.get_value()
                with open(os.path.expanduser("~/.r2_auth"), "w") as f:
                    f.write(r2_auth_prod)

        step(
            name="Write R2 Auth Config",
            command=write_r2_auth,
            workdir=REPO_PATH,
        )

    step(
        name="Prepare Release Info",
        command=[
            f"python3 ./ci/jobs/create_release.py --prepare-release-info"
            f" --ref {args.ref} --release-type {args.release_type}"
            f" {dry_run_flag}".strip()
        ],
        workdir=REPO_PATH,
    )

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
        args.release_type == "patch"
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
                # TODO: re-enable changelog generation
                # "echo 'Generate ChangeLog'",
                # "docker pull clickhouse/style-test:latest",
                # f"git remote set-url origin https://x-access-token:{_GH_TOKEN_SECRET.get_value()}@github.com/ClickHouse/ClickHouse.git",
                # f"CI=1 docker run -u {uid}:{gid} -e PYTHONUNBUFFERED=1 -e CI=1"
                # f" --network=host --volume='{REPO_PATH}:/wd' --workdir=/wd"
                # f" clickhouse/style-test:latest"
                # f" ./tests/ci/changelog.py -v --debug-helpers"
                # f" --gh-user-or-token {_GH_TOKEN_SECRET.get_value()}"
                # f" --jobs=5"
                # f" --output=./docs/changelogs/{release_tag}.md {release_tag}",
                # "git remote set-url origin git@github.com:ClickHouse/ClickHouse.git",
                # f"git add ./docs/changelogs/{release_tag}.md",
                "echo 'Generate Security'",
                "python3 ./utils/security-generator/generate_security.py"
                " > SECURITY.md",
                "git diff HEAD",
            ],
            workdir=REPO_PATH,
        )

    if (
        args.release_type == "patch"
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

            os.environ["GH_TOKEN"] = _GH_TOKEN_SECRET.get_value()
            Shell.check(
                "git config user.email robot-clickhouse@users.noreply.github.com",
                strict=True,
            )
            Shell.check("git config user.name robot-clickhouse", strict=True)
            Shell.check(f"git checkout -b {pr_branch}", strict=True)
            Shell.check("git add -A", strict=True)
            Shell.check(
                f"git commit -m {shlex.quote(commit_msg)}",
                strict=True,
            )
            Shell.check(
                f"git push https://x-access-token:{_GH_TOKEN_SECRET.get_value()}@github.com/ClickHouse/ClickHouse.git {pr_branch}",
                strict=True,
            )

            with tempfile.NamedTemporaryFile(
                mode="w", delete=False, suffix=".txt", encoding="utf-8"
            ) as body_file:
                body_file.write(pr_body)
                body_file_path = body_file.name

            try:
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

    if args.release_type == "patch" and not args.dry_run:
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
                    version_suffix = "" if variant == "ubuntu" else f"-{variant}"
                    label_version = f"{version_string}{version_suffix}"
                    tags = [
                        f"--tag={image}:{version_string}{version_suffix}",
                        f"--tag={image}:{version_minor}{version_suffix}",
                        f"--tag={image}:{version_major}{version_suffix}",
                    ]
                    if is_latest:
                        tags.append(f"--tag={image}:latest{version_suffix}")

                    Shell.check(
                        f"docker buildx build"
                        f" --platform=linux/amd64,linux/arm64"
                        f" --provenance=true"
                        f" --sbom=true"
                        f" --output=type=registry"
                        f" --label=com.clickhouse.build.version={label_version}"
                        f" {' '.join(tags)}"
                        f" --build-arg=VERSION={version_string}"
                        f" --progress=plain"
                        f" --file={dockerfile}"
                        f" {context}",
                        strict=True,
                    )

                Shell.check("git checkout -", strict=True)
                Shell.check(
                    "python3 ./ci/jobs/create_release.py --set-progress-completed",
                    strict=True,
                )

            return build

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
                ],
            ),
            workdir=REPO_PATH,
        )

    # Always run — equivalent to `if: ${{ !cancelled() }}` in the workflow.
    results.append(
        Result.from_commands_run(
            name="Checkout Back",
            command=[f"git checkout {original_branch}"],
            workdir=REPO_PATH,
        )
    )

    results.append(
        Result.from_commands_run(
            name="Update Release Info and Merge Created PRs",
            command=[
                f"python3 ./ci/jobs/create_release.py --merge-prs"
                f" {dry_run_flag}".strip()
            ],
            workdir=REPO_PATH,
        )
    )

    results.append(
        Result.from_commands_run(
            name="Set Release Progress to Completed",
            command=[
                "python3 ./ci/jobs/create_release.py --set-progress-started"
                " --progress completed",
                "python3 ./ci/jobs/create_release.py --set-progress-completed",
            ],
            workdir=REPO_PATH,
        )
    )

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

    Result.create_from(results=results, stopwatch=stopwatch).complete_job()


if __name__ == "__main__":
    main()
