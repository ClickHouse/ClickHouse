"""The docker-library test runner, shared by the docker job scripts.

Kept out of `docker_server.py` so that a caller can reuse it without importing a job entry point,
which would pull in `ci.defs.job_configs` and the rest of that script's module scope.
"""

import logging
import os
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
