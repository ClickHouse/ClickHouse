import re
import shlex

from praktika._environment import _Environment
from praktika.result import Result
from praktika.utils import Shell

DOCKERFILE_PATH = "ci/docker/binary-builder/Dockerfile"
S3_BUCKET = "clickhouse-builds.s3.amazonaws.com"


def update_dockerfile(sha):
    with open(DOCKERFILE_PATH, "r") as f:
        content = f.read()

    new_content = re.sub(
        r"^(ARG TOOLCHAIN_COMMIT=).*$",
        rf"\g<1>{sha}",
        content,
        flags=re.MULTILINE,
    )

    if new_content == content:
        print("TOOLCHAIN_COMMIT ARG not found in the Dockerfile")
        return False

    with open(DOCKERFILE_PATH, "w") as f:
        f.write(new_content)

    return True


def create_pr(sha):
    short_sha = sha[:8]
    branch = f"update-toolchain-{short_sha}"
    amd_url = f"https://{S3_BUCKET}/REFs/master/{sha}/build_toolchain_pgo_bolt_amd64/clang-pgo-bolt.tar.zst"
    arm_url = f"https://{S3_BUCKET}/REFs/master/{sha}/build_toolchain_pgo_bolt_aarch64/clang-pgo-bolt.tar.zst"

    body = f"""\
## Summary
- Update `binary-builder` Dockerfile to use PGO+BOLT optimized toolchain from commit `{short_sha}`
- Artifact URLs:
  - amd64: {amd_url}
  - aarch64: {arm_url}

### Changelog category (leave one):
- Build/Testing/Packaging Improvement

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
- Use PGO+BOLT optimized clang toolchain for CI builds
"""

    commands = [
        f"git checkout -b {branch}",
        f"git add {DOCKERFILE_PATH}",
        f'git commit -m "Update PGO+BOLT toolchain to {short_sha}"',
        f"git push origin {branch}",
    ]

    if not Shell.check(
        " && ".join(commands),
        verbose=True,
    ):
        return False

    if not Shell.check(
        f'gh pr create --title "Update PGO+BOLT toolchain to {short_sha}" '
        f"--body {shlex.quote(body)} "
        f"--base master --head {branch}",
        verbose=True,
    ):
        return False

    return True


if __name__ == "__main__":
    env = _Environment.get()
    sha = env.SHA

    results = []
    res = True

    if res:
        results.append(
            Result.from_commands_run(
                name="Update Dockerfile",
                command=lambda: update_dockerfile(sha),
            )
        )
        res = results[-1].is_ok()

    if res:
        results.append(
            Result.from_commands_run(
                name="Create PR",
                command=lambda: create_pr(sha),
            )
        )
        res = results[-1].is_ok()

    Result.create_from(results=results).complete_job()
