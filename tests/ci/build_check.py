#!/usr/bin/env python3

import argparse
from pathlib import Path
from typing import Tuple
import subprocess
import logging
import sys
import time

from ci_config import CI_CONFIG, BuildConfig
from ccache_utils import CargoCache

from env_helper import (
    GITHUB_JOB_API_URL,
    REPO_COPY,
    S3_BUILDS_BUCKET,
    S3_DOWNLOAD,
    TEMP_PATH,
)
from git_helper import Git, git_runner
from pr_info import PRInfo
from report import BuildResult, FAILURE, StatusType, SUCCESS
from s3_helper import S3Helper
from tee_popen import TeePopen
import docker_images_helper
from version_helper import (
    ClickHouseVersion,
    get_version_from_repo,
    update_version_local,
)
from clickhouse_helper import (
    ClickHouseHelper,
    CiLogsCredentials,
    prepare_tests_results_for_clickhouse,
    get_instance_type,
    get_instance_id,
)
from stopwatch import Stopwatch

IMAGE_NAME = "clickhouse/binary-builder"
BUILD_LOG_NAME = "build_log.log"


def _can_export_binaries(build_config: BuildConfig) -> bool:
    if build_config.package_type != "deb":
        return False
    if build_config.sanitizer != "":
        return True
    if build_config.debug_build:
        return True
    return False


def get_packager_cmd(
    build_config: BuildConfig,
    packager_path: Path,
    output_path: Path,
    cargo_cache_dir: Path,
    build_version: str,
    image_version: str,
    official: bool,
) -> str:
    package_type = build_config.package_type
    comp = build_config.compiler
    cmake_flags = "-DENABLE_CLICKHOUSE_SELF_EXTRACTING=1"
    cmd = (
        f"cd {packager_path} && CMAKE_FLAGS='{cmake_flags}' ./packager "
        f"--output-dir={output_path} --package-type={package_type} --compiler={comp}"
    )

    if build_config.debug_build:
        cmd += " --debug-build"
    if build_config.sanitizer:
        cmd += f" --sanitizer={build_config.sanitizer}"
    if build_config.tidy:
        cmd += " --clang-tidy"

    cmd += " --cache=sccache"
    cmd += " --s3-rw-access"
    cmd += f" --s3-bucket={S3_BUILDS_BUCKET}"
    cmd += f" --cargo-cache-dir={cargo_cache_dir}"

    if build_config.additional_pkgs:
        cmd += " --additional-pkgs"

    cmd += f" --docker-image-version={image_version}"
    cmd += " --with-profiler"
    cmd += f" --version={build_version}"

    if _can_export_binaries(build_config):
        cmd += " --with-binaries=tests"

    if official:
        cmd += " --official"

    return cmd


def build_clickhouse(
    packager_cmd: str, logs_path: Path, build_output_path: Path
) -> Tuple[Path, StatusType]:
    build_log_path = logs_path / BUILD_LOG_NAME
    success = False
    with TeePopen(packager_cmd, build_log_path) as process:
        retcode = process.wait()
        if build_output_path.exists():
            results_exists = any(build_output_path.iterdir())
        else:
            results_exists = False

        if retcode == 0:
            if results_exists:
                success = True
                logging.info("Built successfully")
            else:
                logging.info(
                    "Success exit code, but no build artifacts => build failed"
                )
        else:
            logging.info("Build failed")
    return build_log_path, SUCCESS if success else FAILURE


def check_for_success_run(
    s3_helper: S3Helper,
    s3_prefix: str,
    build_name: str,
    version: ClickHouseVersion,
) -> None:
    # TODO: Remove after S3 artifacts
    logging.info("Checking for artifacts %s in bucket %s", s3_prefix, S3_BUILDS_BUCKET)
    try:
        # Performance artifacts are now part of regular build, so we're safe
        build_results = s3_helper.list_prefix(s3_prefix)
    except Exception as ex:
        logging.info("Got exception while listing %s: %s\nRerun", s3_prefix, ex)
        return

    if build_results is None or len(build_results) == 0:
        logging.info("Nothing found in %s, rerun", s3_prefix)
        return

    logging.info("Some build results found:\n%s", build_results)
    build_urls = []
    log_url = ""
    for url in build_results:
        url_escaped = url.replace("+", "%2B").replace(" ", "%20")
        if BUILD_LOG_NAME in url:
            log_url = f"{S3_DOWNLOAD}/{S3_BUILDS_BUCKET}/{url_escaped}"
        else:
            build_urls.append(f"{S3_DOWNLOAD}/{S3_BUILDS_BUCKET}/{url_escaped}")
    if not log_url:
        # log is uploaded the last, so if there's no log we need to rerun the build
        return

    success = len(build_urls) > 0
    build_result = BuildResult(
        build_name,
        log_url,
        build_urls,
        version.describe,
        SUCCESS if success else FAILURE,
        0,
        GITHUB_JOB_API_URL(),
    )
    result_json_path = build_result.write_json(Path(TEMP_PATH))
    logging.info(
        "Build result file %s is written, content:\n %s",
        result_json_path,
        result_json_path.read_text(encoding="utf-8"),
    )
    # Fail build job if not successeded
    if not success:
        sys.exit(1)
    else:
        sys.exit(0)


def get_release_or_pr(pr_info: PRInfo, version: ClickHouseVersion) -> Tuple[str, str]:
    "Return prefixes for S3 artifacts paths"
    # FIXME performance
    # performance builds are havily relies on a fixed path for artifacts, that's why
    # we need to preserve 0 for anything but PR number
    # It should be fixed in performance-comparison image eventually
    # For performance tests we always set PRs prefix
    performance_pr = "PRs/0"
    if "release" in pr_info.labels or "release-lts" in pr_info.labels:
        # for release pull requests we use branch names prefixes, not pr numbers
        return pr_info.head_ref, performance_pr
    if pr_info.number == 0:
        # for pushes to master - major version
        return f"{version.major}.{version.minor}", performance_pr
    # PR number for anything else
    pr_number = f"PRs/{pr_info.number}"
    return pr_number, pr_number


def upload_master_static_binaries(
    pr_info: PRInfo,
    build_config: BuildConfig,
    s3_helper: S3Helper,
    build_output_path: Path,
) -> None:
    """Upload binary artifacts to a static S3 links"""
    static_binary_name = build_config.static_binary_name
    if pr_info.number != 0:
        return
    elif not static_binary_name:
        return
    elif pr_info.base_ref != "master":
        return

    # Full binary with debug info:
    s3_path_full = "/".join((pr_info.base_ref, static_binary_name, "clickhouse-full"))
    binary_full = build_output_path / "clickhouse"
    url_full = s3_helper.upload_build_file_to_s3(binary_full, s3_path_full)
    print(f"::notice ::Binary static URL (with debug info): {url_full}")

    # Stripped binary without debug info:
    s3_path_compact = "/".join((pr_info.base_ref, static_binary_name, "clickhouse"))
    binary_compact = build_output_path / "clickhouse-stripped"
    url_compact = s3_helper.upload_build_file_to_s3(binary_compact, s3_path_compact)
    print(f"::notice ::Binary static URL (compact): {url_compact}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser("Clickhouse builder script")
    parser.add_argument(
        "--tag",
        required=False,
        default="",
        type=str,
        help="tag for docker image",
    )
    parser.add_argument(
        "--build-name",
        required=True,
        type=str,
        help="build name",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)

    args = parse_args()

    stopwatch = Stopwatch()
    build_name = args.build_name

    build_config = CI_CONFIG.build_config[build_name]

    temp_path = Path(TEMP_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)
    repo_path = Path(REPO_COPY)

    pr_info = PRInfo()

    logging.info("Repo copy path %s", repo_path)

    s3_helper = S3Helper()

    version = get_version_from_repo(git=Git(True))
    release_or_pr, performance_pr = get_release_or_pr(pr_info, version)

    s3_path_prefix = "/".join((release_or_pr, pr_info.sha, build_name))
    # FIXME performance
    s3_performance_path = "/".join(
        (performance_pr, pr_info.sha, build_name, "performance.tar.zst")
    )

    # FIXME: to be removed in favor of "skip by job digest"
    # If this is rerun, then we try to find already created artifacts and just
    # put them as github actions artifact (result)
    # The s3_path_prefix has additional "/" in the end to prevent finding
    # e.g. `binary_darwin_aarch64/clickhouse` for `binary_darwin`
    check_for_success_run(s3_helper, f"{s3_path_prefix}/", build_name, version)

    logging.info("Got version from repo %s", version.string)

    official_flag = pr_info.number == 0

    version_type = "testing"
    if "release" in pr_info.labels or "release-lts" in pr_info.labels:
        version_type = "stable"
        official_flag = True

    update_version_local(version, version_type)

    logging.info("Updated local files with version")

    logging.info("Build short name %s", build_name)

    build_output_path = temp_path / build_name
    build_output_path.mkdir(parents=True, exist_ok=True)
    cargo_cache = CargoCache(
        temp_path / "cargo_cache" / "registry", temp_path, s3_helper
    )
    cargo_cache.download()

    docker_image = docker_images_helper.pull_image(
        docker_images_helper.get_docker_image(IMAGE_NAME)
    )

    packager_cmd = get_packager_cmd(
        build_config,
        repo_path / "docker" / "packager",
        build_output_path,
        cargo_cache.directory,
        version.string,
        docker_image.version,
        official_flag,
    )

    logging.info("Going to run packager with %s", packager_cmd)

    logs_path = temp_path / "build_log"
    logs_path.mkdir(parents=True, exist_ok=True)

    start = time.time()
    log_path, build_status = build_clickhouse(
        packager_cmd, logs_path, build_output_path
    )
    elapsed = int(time.time() - start)
    subprocess.check_call(
        f"sudo chown -R ubuntu:ubuntu {build_output_path}", shell=True
    )
    logging.info("Build finished as %s, log path %s", build_status, log_path)
    if build_status == SUCCESS:
        cargo_cache.upload()
    else:
        # We check if docker works, because if it's down, it's infrastructure
        try:
            subprocess.check_call("docker info", shell=True)
        except subprocess.CalledProcessError:
            logging.error(
                "The dockerd looks down, won't upload anything and generate report"
            )
            sys.exit(1)

    # FIXME performance
    performance_urls = []
    performance_path = build_output_path / "performance.tar.zst"
    if performance_path.exists():
        performance_urls.append(
            s3_helper.upload_build_file_to_s3(performance_path, s3_performance_path)
        )
        logging.info(
            "Uploaded performance.tar.zst to %s, now delete to avoid duplication",
            performance_urls[0],
        )
        performance_path.unlink()

    build_urls = (
        s3_helper.upload_build_directory_to_s3(
            build_output_path,
            s3_path_prefix,
            keep_dirs_in_s3_path=False,
            upload_symlinks=False,
        )
        + performance_urls
    )
    logging.info("Got build URLs %s", build_urls)

    print("::notice ::Build URLs: {}".format("\n".join(build_urls)))

    if log_path.exists():
        log_url = s3_helper.upload_build_file_to_s3(
            log_path, s3_path_prefix + "/" + log_path.name
        )
        logging.info("Log url %s", log_url)
    else:
        logging.info("Build log doesn't exist")

    print(f"::notice ::Log URL: {log_url}")

    build_result = BuildResult(
        build_name,
        log_url,
        build_urls,
        version.describe,
        build_status,
        elapsed,
        GITHUB_JOB_API_URL(),
    )
    result_json_path = build_result.write_json(temp_path)
    logging.info(
        "Build result file %s is written, content:\n %s",
        result_json_path,
        result_json_path.read_text(encoding="utf-8"),
    )

    upload_master_static_binaries(pr_info, build_config, s3_helper, build_output_path)

    # Upload profile data
    ch_helper = ClickHouseHelper()

    ci_logs_credentials = CiLogsCredentials(Path("/dev/null"))
    if ci_logs_credentials.host:
        instance_type = get_instance_type()
        instance_id = get_instance_id()
        query = f"""INSERT INTO build_time_trace
(
    pull_request_number,
    commit_sha,
    check_start_time,
    check_name,
    instance_type,
    instance_id,
    file,
    library,
    time,
    pid,
    tid,
    ph,
    ts,
    dur,
    cat,
    name,
    detail,
    count,
    avgMs,
    args_name
)
SELECT {pr_info.number}, '{pr_info.sha}', '{stopwatch.start_time_str}', '{build_name}', '{instance_type}', '{instance_id}', *
FROM input('
    file String,
    library String,
    time DateTime64(6),
    pid UInt32,
    tid UInt32,
    ph String,
    ts UInt64,
    dur UInt64,
    cat String,
    name String,
    detail String,
    count UInt64,
    avgMs UInt64,
    args_name String')
FORMAT JSONCompactEachRow"""

        auth = {
            "X-ClickHouse-User": "ci",
            "X-ClickHouse-Key": ci_logs_credentials.password,
        }
        url = f"https://{ci_logs_credentials.host}/"
        profiles_dir = temp_path / "profiles_source"
        profiles_dir.mkdir(parents=True, exist_ok=True)
        logging.info(
            "Processing profile JSON files from %s", repo_path / "build_docker"
        )
        git_runner(
            "./utils/prepare-time-trace/prepare-time-trace.sh "
            f"build_docker {profiles_dir.absolute()}"
        )
        profile_data_file = temp_path / "profile.json"
        with open(profile_data_file, "wb") as profile_fd:
            for profile_source in profiles_dir.iterdir():
                if profile_source.name != "binary_sizes.txt":
                    with open(profiles_dir / profile_source, "rb") as ps_fd:
                        profile_fd.write(ps_fd.read())

        logging.info(
            "::notice ::Log Uploading profile data, path: %s, size: %s, query: %s",
            profile_data_file,
            profile_data_file.stat().st_size,
            query,
        )
        ch_helper.insert_file(url, auth, query, profile_data_file)

        query = f"""INSERT INTO binary_sizes
(
    pull_request_number,
    commit_sha,
    check_start_time,
    check_name,
    instance_type,
    instance_id,
    file,
    size
)
SELECT {pr_info.number}, '{pr_info.sha}', '{stopwatch.start_time_str}', '{build_name}', '{instance_type}', '{instance_id}', file, size
FROM input('size UInt64, file String')
SETTINGS format_regexp = '^\\s*(\\d+) (.+)$'
FORMAT Regexp"""

        binary_sizes_file = profiles_dir / "binary_sizes.txt"

        logging.info(
            "::notice ::Log Uploading binary sizes data, path: %s, size: %s, query: %s",
            binary_sizes_file,
            binary_sizes_file.stat().st_size,
            query,
        )
        ch_helper.insert_file(url, auth, query, binary_sizes_file)

    # Upload statistics to CI database
    prepared_events = prepare_tests_results_for_clickhouse(
        pr_info,
        [],
        build_status,
        stopwatch.duration_seconds,
        stopwatch.start_time_str,
        log_url,
        f"Build ({build_name})",
    )
    ch_helper.insert_events_into(db="default", table="checks", events=prepared_events)

    # Fail the build job if it didn't succeed
    if build_status != SUCCESS:
        sys.exit(1)


if __name__ == "__main__":
    main()
