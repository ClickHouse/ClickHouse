"""The docker-library test runner, shared by the docker job scripts.

Kept out of `docker_server.py` so that a caller can reuse it without importing a job entry point,
which would pull in `ci.defs.job_configs` and the rest of that script's module scope.
"""

import logging
import os
import re
import shlex
import tempfile
import traceback
from pathlib import Path

from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

GITHUB_SERVER_URL = os.getenv("GITHUB_SERVER_URL", "https://github.com")

# Derived from this file, not the working directory: a caller in another directory would otherwise
# get a clone path outside `ci/tmp` and a `config.sh` that does not exist.
SCRIPT_DIR = Path(__file__).resolve().parent  # ci/jobs/scripts/docker_server
CONFIG_OVERRIDE = SCRIPT_DIR / "config.sh"
TEMP_PATH = SCRIPT_DIR.parents[2] / "tmp"  # parents[2] is `ci`
REPO_ROOT = SCRIPT_DIR.parents[3]
OFFICIAL_IMAGES_REPO = "docker-library/official-images"

# A test is a `clickhouse-*` or `keeper-*` directory, the names `config.sh` runs; `tests/lib.sh` is a shared helper.
_TEST_FILE_RE = re.compile(
    r"ci/jobs/scripts/docker_server/tests/((?:clickhouse|keeper)-[^/]+)/"
)


def changed_tests(changed_files) -> list:
    """Names of the tests with a changed file that still exists."""
    tests = []
    for file in changed_files or []:
        file = file.removeprefix(".").removeprefix("/")
        match = _TEST_FILE_RE.match(file)
        if match and (REPO_ROOT / file).is_file() and match.group(1) not in tests:
            tests.append(match.group(1))
    return tests


def is_distroless_image(docker_image: str) -> bool:
    _, tag = docker_image.rsplit(":", 1)
    return "distroless" in tag.split("-")


def get_official_images_variant(docker_image: str) -> str:
    # The official-images test runner derives its lookup variant from the final
    # tag suffix. For example, head-distroless-amd64 is looked up as repo:amd64.
    _, tag = docker_image.rsplit(":", 1)
    return tag.rsplit("-", 1)[-1]


def write_distroless_docker_library_config(docker_image: str, config_dir: Path) -> Path:
    """Map arch-suffixed distroless tags to the distroless-safe config tests."""
    # Generate a short config fragment for local arch-suffixed distroless CI tags.
    # The runner derives tags like head-distroless-amd64 as repo:amd64; map that
    # derived key to the distroless-safe tests because this helper is only used
    # for images already identified as distroless.
    repo, _ = docker_image.rsplit(":", 1)
    variant = get_official_images_variant(docker_image)
    image_variant = shlex.quote(f"{repo}:{variant}")
    tests_var = (
        "keeperDistrolessSafeTests"
        if "clickhouse-keeper" in repo
        else "clickhouseDistrolessSafeTests"
    )

    generated_config = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            prefix="docker-library-distroless-",
            suffix=".sh",
            dir=config_dir,
            delete=False,
            encoding="utf-8",
        ) as f:
            generated_config = Path(f.name)
            f.write(
                "#!/usr/bin/env bash\n"
                "\n"
                "explicitTests+=(\n"
                f"\t[{image_variant}]=1\n"
                ")\n"
                "\n"
                "imageTests+=(\n"
                f'\t[{image_variant}]="${{{tests_var}}}"\n'
                ")\n"
            )
            return generated_config
    except Exception:
        if generated_config:
            generated_config.unlink(missing_ok=True)
        raise


def test_docker_library(test_results, check_images=None) -> None:
    """we test our images vs the official docker library repository to track integrity

    `check_images` names the images to run the suite against. Without it they are taken from the
    `-<arch>`-suffixed `test_results` names, which is how this job labels them; a caller using
    another naming convention passes its own tags, so a mismatch cannot silently test nothing.
    """
    arch = "amd64" if Utils.is_amd() else "arm64"
    if check_images is None:
        check_images = [tr.name for tr in test_results if tr.name.endswith(f"-{arch}")]
    if not check_images:
        return
    test_name = "docker library image test"
    try:
        repo = "docker-library/official-images"
        logging.info("Cloning %s repository to run tests for 'clickhouse' image", repo)
        repo_path = TEMP_PATH / repo
        if not Shell.check(
            f"git clone --depth 1 {GITHUB_SERVER_URL}/{repo} {repo_path}",
            verbose=True,
            retries=3,
        ):
            raise RuntimeError(f"Failed to clone {repo}")
        run_sh = repo_path / "test/run.sh"
        for image in check_images:
            generated_config = None
            try:
                configs = [repo_path / "test/config.sh", CONFIG_OVERRIDE]
                if is_distroless_image(image):
                    generated_config = write_distroless_docker_library_config(
                        image, CONFIG_OVERRIDE.parent
                    )
                    configs.append(generated_config)
                config_args = " ".join(
                    f"-c {shlex.quote(config.as_posix())}" for config in configs
                )
                cmd = f"{shlex.quote(run_sh.as_posix())} {shlex.quote(image)} {config_args}"
                test_results.append(
                    Result.from_commands_run(
                        name=f"{test_name} ({image})", command=cmd, with_info=True
                    )
                )
            finally:
                if generated_config:
                    generated_config.unlink(missing_ok=True)

    except Exception as e:
        logging.error("Failed while testing the docker library image: %s", e)
        test_results.append(
            Result(
                name=test_name,
                status=Result.Status.FAIL,
                info=f"Exception while testing docker library: {traceback.format_exc()}",
            )
        )


def validate_bugfix_tests(test_results, master_images, tests) -> None:
    """Run a bug fix's changed tests on the images built from master, where they must fail.

    A test that passes on every image whose `config.sh` list has it does not reproduce the bug. Like the functional
    tests' Bugfix validation, failing on one image is enough: a bug can be in one variant only.
    """
    # Cloned by `test_docker_library`, which reports a failed clone itself.
    repo_path = TEMP_PATH / OFFICIAL_IMAGES_REPO
    run_sh = repo_path / "test/run.sh"
    configs = f"-c {shlex.quote((repo_path / 'test/config.sh').as_posix())} -c {shlex.quote(CONFIG_OVERRIDE.as_posix())}"
    failed_on = {test: [] for test in tests}
    passed_on = {test: [] for test in tests}
    logs = {test: [] for test in tests}
    pulled_all = True
    for image in master_images:
        if not Shell.check(
            f"docker pull {shlex.quote(image)}", verbose=True, retries=3
        ):
            test_results.append(
                Result(
                    name=f"bugfix validation ({image})",
                    status=Result.Status.ERROR,
                    info=f"Failed to pull {image}",
                )
            )
            pulled_all = False
            continue
        for test in tests:
            cmd = f"{shlex.quote(run_sh.as_posix())} -t {shlex.quote(test)} {shlex.quote(image)} {configs}"
            exit_code, stdout, stderr = Shell.get_res_stdout_stderr(cmd, verbose=True)
            if "image has no tests" in stdout:
                continue
            if exit_code == 0:
                passed_on[test].append(image)
            elif f"'{test}' [" in stdout:
                failed_on[test].append(image)
                logs[test].extend(part for part in (stdout, stderr) if part)
            else:
                test_results.append(
                    Result(
                        name=f"bugfix validation: {test} ({image})",
                        status=Result.Status.ERROR,
                        info="\n".join(
                            part
                            for part in (
                                f"{test} did not run on {image}",
                                stdout,
                                stderr,
                            )
                            if part
                        ),
                    )
                )
    for test in tests:
        if failed_on[test]:
            status = Result.Status.OK
            info = f"{test} fails on {', '.join(failed_on[test])}, built from master, as expected"
        elif passed_on[test]:
            status = Result.Status.FAIL
            info = f"{test} passes on {', '.join(passed_on[test])}, built from master, so it does not reproduce the bug"
        elif pulled_all:
            status = Result.Status.ERROR
            info = f"{test} runs on none of {', '.join(master_images)}"
        else:
            continue
        test_results.append(
            Result(
                name=f"bugfix validation: {test}",
                status=status,
                info="\n".join([info, *logs[test]]),
            )
        )
