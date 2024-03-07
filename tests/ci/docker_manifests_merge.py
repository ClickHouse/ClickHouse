#!/usr/bin/env python3

import argparse
import json
import logging
import os
import subprocess

from pathlib import Path
from typing import List, Dict, Tuple

from github import Github

from clickhouse_helper import (
    ClickHouseHelper,
    prepare_tests_results_for_clickhouse,
    CHException,
)
from commit_status_helper import format_description, get_commit, post_commit_status
from docker_images_helper import IMAGES_FILE_PATH, get_image_names
from env_helper import RUNNER_TEMP, REPO_COPY
from get_robot_token import get_best_robot_token, get_parameter_from_ssm
from git_helper import Runner
from pr_info import PRInfo
from report import TestResults, TestResult
from s3_helper import S3Helper
from stopwatch import Stopwatch
from upload_result_helper import upload_results

NAME = "Push multi-arch images to Dockerhub"
CHANGED_IMAGES = "changed_images_{}.json"
Images = Dict[str, List[str]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="The program gets images from changed_images_*.json, merges images "
        "with different architectures into one manifest and pushes back to docker hub",
    )

    parser.add_argument(
        "--suffix",
        dest="suffixes",
        type=str,
        required=True,
        action="append",
        help="suffixes for existing images' tags. More than two should be given",
    )
    parser.add_argument(
        "--path",
        type=Path,
        default=RUNNER_TEMP,
        help="path to changed_images_*.json files",
    )
    parser.add_argument("--reports", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-reports",
        action="store_false",
        dest="reports",
        default=argparse.SUPPRESS,
        help="don't push reports to S3 and github",
    )
    parser.add_argument("--push", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-push-images",
        action="store_false",
        dest="push",
        default=argparse.SUPPRESS,
        help="don't push images to docker hub",
    )

    args = parser.parse_args()
    if len(args.suffixes) < 2:
        parser.error("more than two --suffix should be given")

    return args


def load_images(path: Path, suffix: str) -> Images:
    with open(path / CHANGED_IMAGES.format(suffix), "rb") as images:
        return json.load(images)  # type: ignore


def strip_suffix(suffix: str, images: Images) -> Images:
    result = {}
    for image, versions in images.items():
        for v in versions:
            if not v.endswith(f"-{suffix}"):
                raise ValueError(
                    f"version {image}:{v} does not contain suffix {suffix}"
                )
        result[image] = [v[: -len(suffix) - 1] for v in versions]

    return result


def check_sources(to_merge: Dict[str, Images]) -> Images:
    """get a dict {arch1: Images, arch2: Images}"""
    result = {}  # type: Images
    first_suffix = ""
    for suffix, images in to_merge.items():
        if not result:
            first_suffix = suffix
            result = strip_suffix(suffix, images)
            continue
        if not result == strip_suffix(suffix, images):
            raise ValueError(
                f"images in {images} are not equal to {to_merge[first_suffix]}"
            )

    return result


def get_changed_images(images: Images) -> Dict[str, str]:
    """The original json format is {"image": "tag"}, so the output artifact is
    produced here. The latest version is {PR_NUMBER}-{SHA1}
    """
    return {k: v[-1] for k, v in images.items()}


def merge_images(to_merge: Dict[str, Images]) -> Dict[str, List[List[str]]]:
    """The function merges image-name:version-suffix1 and image-name:version-suffix2
    into image-name:version"""
    suffixes = to_merge.keys()
    result_images = check_sources(to_merge)
    merge = {}  # type: Dict[str, List[List[str]]]

    for image, versions in result_images.items():
        merge[image] = []
        for i, v in enumerate(versions):
            merged_v = [v]  # type: List[str]
            for suf in suffixes:
                merged_v.append(to_merge[suf][image][i])
            merge[image].append(merged_v)

    return merge


def create_manifest(image: str, tags: List[str], push: bool) -> Tuple[str, str]:
    tag = tags[0]
    manifest = f"{image}:{tag}"
    cmd = "docker manifest create --amend " + " ".join((f"{image}:{t}" for t in tags))
    logging.info("running: %s", cmd)
    with subprocess.Popen(
        cmd,
        shell=True,
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
        universal_newlines=True,
    ) as popen:
        retcode = popen.wait()
        if retcode != 0:
            output = popen.stdout.read()  # type: ignore
            logging.error("failed to create manifest for %s:\n %s\n", manifest, output)
            return manifest, "FAIL"
        if not push:
            return manifest, "OK"

    cmd = f"docker manifest push {manifest}"
    logging.info("running: %s", cmd)
    with subprocess.Popen(
        cmd,
        shell=True,
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
        universal_newlines=True,
    ) as popen:
        retcode = popen.wait()
        if retcode != 0:
            output = popen.stdout.read()  # type: ignore
            logging.error("failed to push %s:\n %s\n", manifest, output)
            return manifest, "FAIL"

    return manifest, "OK"


def enrich_images(changed_images: Dict[str, str]) -> None:
    all_image_names = get_image_names(Path(REPO_COPY), IMAGES_FILE_PATH)

    images_to_find_tags_for = [
        image for image in all_image_names if image not in changed_images
    ]
    images_to_find_tags_for.sort()

    logging.info(
        "Trying to find versions for images:\n %s", "\n ".join(images_to_find_tags_for)
    )

    COMMIT_SHA_BATCH_SIZE = 100
    MAX_COMMIT_BATCHES_TO_CHECK = 10
    # Gets the sha of the last COMMIT_SHA_BATCH_SIZE commits after skipping some commits (see below)
    LAST_N_ANCESTOR_SHA_COMMAND = f"git log --format=format:'%H' --max-count={COMMIT_SHA_BATCH_SIZE} --skip={{}} --merges"
    git_runner = Runner()

    GET_COMMIT_SHAS_QUERY = """
        WITH {commit_shas:Array(String)} AS commit_shas,
             {images:Array(String)} AS images
        SELECT
            splitByChar(':', test_name)[1] AS image_name,
            argMax(splitByChar(':', test_name)[2], check_start_time) AS tag
        FROM checks
            WHERE
                check_name == 'Push multi-arch images to Dockerhub'
                AND position(test_name, checks.commit_sha)
                AND checks.commit_sha IN commit_shas
                AND image_name IN images
        GROUP BY image_name
        """

    batch_count = 0
    # We use always publicly available DB here intentionally
    ch_helper = ClickHouseHelper(
        "https://play.clickhouse.com", {"X-ClickHouse-User": "play"}
    )

    while (
        batch_count <= MAX_COMMIT_BATCHES_TO_CHECK and len(images_to_find_tags_for) != 0
    ):
        commit_shas = git_runner(
            LAST_N_ANCESTOR_SHA_COMMAND.format(batch_count * COMMIT_SHA_BATCH_SIZE)
        ).split("\n")

        result = ch_helper.select_json_each_row(
            "default",
            GET_COMMIT_SHAS_QUERY,
            {"commit_shas": commit_shas, "images": images_to_find_tags_for},
        )
        result.sort(key=lambda x: x["image_name"])

        logging.info(
            "Found images for commits %s..%s:\n %s",
            commit_shas[0],
            commit_shas[-1],
            "\n ".join(f"{im['image_name']}:{im['tag']}" for im in result),
        )

        for row in result:
            image_name = row["image_name"]
            changed_images[image_name] = row["tag"]
            images_to_find_tags_for.remove(image_name)

        batch_count += 1


def main():
    logging.basicConfig(level=logging.INFO)
    stopwatch = Stopwatch()

    args = parse_args()
    if args.push:
        subprocess.check_output(  # pylint: disable=unexpected-keyword-arg
            "docker login --username 'robotclickhouse' --password-stdin",
            input=get_parameter_from_ssm("dockerhub_robot_password"),
            encoding="utf-8",
            shell=True,
        )

    to_merge = {}
    for suf in args.suffixes:
        to_merge[suf] = load_images(args.path, suf)

    changed_images = get_changed_images(check_sources(to_merge))

    os.environ["DOCKER_CLI_EXPERIMENTAL"] = "enabled"
    merged = merge_images(to_merge)

    status = "success"
    test_results = []  # type: TestResults
    for image, versions in merged.items():
        for tags in versions:
            manifest, test_result = create_manifest(image, tags, args.push)
            test_results.append(TestResult(manifest, test_result))
            if test_result != "OK":
                status = "failure"

    enriched_images = changed_images.copy()
    try:
        # changed_images now contains all the images that are changed in this PR. Let's find the latest tag for the images that are not changed.
        enrich_images(enriched_images)
    except CHException as ex:
        logging.warning("Couldn't get proper tags for not changed images: %s", ex)

    with open(args.path / "changed_images.json", "w", encoding="utf-8") as ci:
        json.dump(enriched_images, ci)

    pr_info = PRInfo()
    s3_helper = S3Helper()

    url = upload_results(s3_helper, pr_info.number, pr_info.sha, test_results, [], NAME)

    print(f"::notice ::Report url: {url}")

    if not args.reports:
        return

    if changed_images:
        description = "Updated " + ", ".join(changed_images.keys())
    else:
        description = "Nothing to update"

    description = format_description(description)

    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)
    post_commit_status(commit, status, url, description, NAME, pr_info)

    prepared_events = prepare_tests_results_for_clickhouse(
        pr_info,
        test_results,
        status,
        stopwatch.duration_seconds,
        stopwatch.start_time_str,
        url,
        NAME,
    )
    ch_helper = ClickHouseHelper()
    ch_helper.insert_events_into(db="default", table="checks", events=prepared_events)


if __name__ == "__main__":
    main()
