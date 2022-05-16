#!/usr/bin/env python3
#
import subprocess
import logging
import json
import os
import sys
import time
from shutil import rmtree
from typing import List, Optional, Tuple

from env_helper import REPO_COPY, TEMP_PATH, CACHES_PATH, IMAGES_PATH
from s3_helper import S3Helper
from pr_info import PRInfo
from version_helper import (
    ClickHouseVersion,
    Git,
    get_version_from_repo,
    update_version_local,
)
from ccache_utils import get_ccache_if_not_exists, upload_ccache
from ci_config import CI_CONFIG, BuildConfig
from docker_pull_helper import get_image_with_version
from tee_popen import TeePopen

IMAGE_NAME = "clickhouse/binary-builder"


def _can_export_binaries(build_config: BuildConfig) -> bool:
    if build_config["package_type"] != "deb":
        return False
    if build_config["bundled"] != "bundled":
        return False
    if build_config["splitted"] == "splitted":
        return False
    if build_config["sanitizer"] != "":
        return True
    if build_config["build_type"] != "":
        return True
    return False


def get_packager_cmd(
    build_config: BuildConfig,
    packager_path: str,
    output_path: str,
    build_version: str,
    image_version: str,
    ccache_path: str,
    official: bool,
) -> str:
    package_type = build_config["package_type"]
    comp = build_config["compiler"]
    cmd = (
        f"cd {packager_path} && ./packager --output-dir={output_path} "
        f"--package-type={package_type} --compiler={comp}"
    )

    if build_config["build_type"]:
        cmd += f" --build-type={build_config['build_type']}"
    if build_config["sanitizer"]:
        cmd += f" --sanitizer={build_config['sanitizer']}"
    if build_config["splitted"] == "splitted":
        cmd += " --split-binary"
    if build_config["tidy"] == "enable":
        cmd += " --clang-tidy"

    cmd += " --cache=ccache"
    cmd += f" --ccache_dir={ccache_path}"

    if "additional_pkgs" in build_config and build_config["additional_pkgs"]:
        cmd += " --additional-pkgs"

    cmd += f" --docker-image-version={image_version}"
    cmd += f" --version={build_version}"

    if _can_export_binaries(build_config):
        cmd += " --with-binaries=tests"

    if official:
        cmd += " --official"

    return cmd


def build_clickhouse(
    packager_cmd: str, logs_path: str, build_output_path: str
) -> Tuple[str, bool]:
    build_log_path = os.path.join(logs_path, "build_log.log")
    success = False
    with TeePopen(packager_cmd, build_log_path) as process:
        retcode = process.wait()
        if os.path.exists(build_output_path):
            build_results = os.listdir(build_output_path)
        else:
            build_results = []

        if retcode == 0:
            if len(build_results) > 0:
                success = True
                logging.info("Built successfully")
            else:
                logging.info(
                    "Success exit code, but no build artifacts => build failed"
                )
        else:
            logging.info("Build failed")
    return build_log_path, success


def get_build_results_if_exists(
    s3_helper: S3Helper, s3_prefix: str
) -> Optional[List[str]]:
    try:
        content = s3_helper.list_prefix(s3_prefix)
        return content
    except Exception as ex:
        logging.info("Got exception %s listing %s", ex, s3_prefix)
        return None


def create_json_artifact(
    temp_path: str,
    build_name: str,
    log_url: str,
    build_urls: List[str],
    build_config: BuildConfig,
    elapsed: int,
    success: bool,
):
    subprocess.check_call(
        f"echo 'BUILD_URLS=build_urls_{build_name}' >> $GITHUB_ENV", shell=True
    )

    result = {
        "log_url": log_url,
        "build_urls": build_urls,
        "build_config": build_config,
        "elapsed_seconds": elapsed,
        "status": success,
    }

    json_name = "build_urls_" + build_name + ".json"

    print(f"Dump json report {result} to {json_name} with env build_urls_{build_name}")

    with open(os.path.join(temp_path, json_name), "w", encoding="utf-8") as build_links:
        json.dump(result, build_links)


def get_release_or_pr(
    pr_info: PRInfo, build_config: BuildConfig, version: ClickHouseVersion
) -> str:
    if "release" in pr_info.labels or "release-lts" in pr_info.labels:
        # for release pull requests we use branch names prefixes, not pr numbers
        return pr_info.head_ref
    elif pr_info.number == 0 and build_config["package_type"] != "performance":
        # for pushes to master - major version, but not for performance builds
        # they havily relies on a fixed path for build package and nobody going
        # to deploy them somewhere, so it's ok.
        return f"{version.major}.{version.minor}"
    # PR number for anything else
    return str(pr_info.number)


def upload_master_static_binaries(
    pr_info: PRInfo,
    build_config: BuildConfig,
    s3_helper: S3Helper,
    build_output_path: str,
):
    """Upload binary artifacts to a static S3 links"""
    static_binary_name = build_config.get("static_binary_name", False)
    if pr_info.number != 0:
        return
    elif not static_binary_name:
        return
    elif pr_info.base_ref != "master":
        return

    s3_path = "/".join((pr_info.base_ref, static_binary_name, "clickhouse"))
    binary = os.path.join(build_output_path, "clickhouse")
    url = s3_helper.upload_build_file_to_s3(binary, s3_path)
    print(f"::notice ::Binary static URL: {url}")


def main():
    logging.basicConfig(level=logging.INFO)

    build_name = sys.argv[1]

    build_config = CI_CONFIG["build_config"][build_name]

    if not os.path.exists(TEMP_PATH):
        os.makedirs(TEMP_PATH)

    pr_info = PRInfo()

    logging.info("Repo copy path %s", REPO_COPY)

    s3_helper = S3Helper("https://s3.amazonaws.com")

    version = get_version_from_repo(git=Git(True))
    release_or_pr = get_release_or_pr(pr_info, build_config, version)

    s3_path_prefix = "/".join((release_or_pr, pr_info.sha, build_name))

    # If this is rerun, then we try to find already created artifacts and just
    # put them as github actions artifcat (result)
    build_results = get_build_results_if_exists(s3_helper, s3_path_prefix)
    if build_results is not None and len(build_results) > 0:
        logging.info("Some build results found %s", build_results)
        build_urls = []
        log_url = ""
        for url in build_results:
            if "build_log.log" in url:
                log_url = "https://s3.amazonaws.com/clickhouse-builds/" + url.replace(
                    "+", "%2B"
                ).replace(" ", "%20")
            else:
                build_urls.append(
                    "https://s3.amazonaws.com/clickhouse-builds/"
                    + url.replace("+", "%2B").replace(" ", "%20")
                )
        success = len(build_urls) > 0
        create_json_artifact(
            TEMP_PATH,
            build_name,
            log_url,
            build_urls,
            build_config,
            0,
            success,
        )
        # Fail build job if not successeded
        if not success:
            sys.exit(1)
        else:
            sys.exit(0)

    docker_image = get_image_with_version(IMAGES_PATH, IMAGE_NAME)
    image_version = docker_image.version

    logging.info("Got version from repo %s", version.string)

    official_flag = pr_info.number == 0
    if "official" in build_config:
        official_flag = build_config["official"]

    version_type = "testing"
    if "release" in pr_info.labels or "release-lts" in pr_info.labels:
        version_type = "stable"
        official_flag = True

    update_version_local(version, version_type)

    logging.info("Updated local files with version")

    logging.info("Build short name %s", build_name)

    build_output_path = os.path.join(TEMP_PATH, build_name)
    if not os.path.exists(build_output_path):
        os.makedirs(build_output_path)

    ccache_path = os.path.join(CACHES_PATH, build_name + "_ccache")

    logging.info("Will try to fetch cache for our build")
    try:
        get_ccache_if_not_exists(ccache_path, s3_helper, pr_info.number, TEMP_PATH)
    except Exception as e:
        # In case there are issues with ccache, remove the path and do not fail a build
        logging.info("Failed to get ccache, building without it. Error: %s", e)
        rmtree(ccache_path, ignore_errors=True)

    if not os.path.exists(ccache_path):
        logging.info("cache was not fetched, will create empty dir")
        os.makedirs(ccache_path)

    packager_cmd = get_packager_cmd(
        build_config,
        os.path.join(REPO_COPY, "docker/packager"),
        build_output_path,
        version.string,
        image_version,
        ccache_path,
        official_flag,
    )

    logging.info("Going to run packager with %s", packager_cmd)

    build_clickhouse_log = os.path.join(TEMP_PATH, "build_log")
    if not os.path.exists(build_clickhouse_log):
        os.makedirs(build_clickhouse_log)

    start = time.time()
    log_path, success = build_clickhouse(
        packager_cmd, build_clickhouse_log, build_output_path
    )
    elapsed = int(time.time() - start)
    subprocess.check_call(
        f"sudo chown -R ubuntu:ubuntu {build_output_path}", shell=True
    )
    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {ccache_path}", shell=True)
    logging.info("Build finished with %s, log path %s", success, log_path)

    logging.info("Will upload cache")
    upload_ccache(ccache_path, s3_helper, pr_info.number, TEMP_PATH)

    if os.path.exists(log_path):
        log_url = s3_helper.upload_build_file_to_s3(
            log_path, s3_path_prefix + "/" + os.path.basename(log_path)
        )
        logging.info("Log url %s", log_url)
    else:
        logging.info("Build log doesn't exist")

    build_urls = s3_helper.upload_build_folder_to_s3(
        build_output_path,
        s3_path_prefix,
        keep_dirs_in_s3_path=False,
        upload_symlinks=False,
    )
    logging.info("Got build URLs %s", build_urls)

    print("::notice ::Build URLs: {}".format("\n".join(build_urls)))

    print(f"::notice ::Log URL: {log_url}")

    create_json_artifact(
        TEMP_PATH, build_name, log_url, build_urls, build_config, elapsed, success
    )

    upload_master_static_binaries(pr_info, build_config, s3_helper, build_output_path)
    # Fail build job if not successeded
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
