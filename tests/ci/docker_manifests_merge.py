#!/usr/bin/env python3

import argparse
import json
import logging
import os
import subprocess

import sys
from typing import List, Tuple

from github import Github

from clickhouse_helper import (
    ClickHouseHelper,
    prepare_tests_results_for_clickhouse,
)
from commit_status_helper import format_description, get_commit, post_commit_status
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from report import TestResult
from s3_helper import S3Helper
from stopwatch import Stopwatch
from env_helper import ROOT_DIR
from upload_result_helper import upload_results
from docker_images_helper import docker_login, get_images_oredered_list

NAME = "Push multi-arch images to Dockerhub"


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
        "--missing-images",
        type=str,
        required=True,
        help="json string or json file with images to build {IMAGE: TAG} or type all to build all",
    )
    parser.add_argument(
        "--image-tags",
        type=str,
        required=True,
        help="json string or json file with all images and their tags {IMAGE: TAG}",
    )
    parser.add_argument(
        "--set-latest",
        type=str,
        help="add latest tag",
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


def create_manifest(
    image: str, result_tag: str, tags: List[str], push: bool
) -> Tuple[str, str]:
    manifest = f"{image}:{result_tag}"
    cmd = "docker manifest create --amend " + " ".join(
        (f"{image}:{t}" for t in [result_tag] + tags)
    )
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


def main():
    # to be aligned with docker paths from image.json
    os.chdir(ROOT_DIR)
    logging.basicConfig(level=logging.INFO)
    stopwatch = Stopwatch()

    args = parse_args()

    if args.push:
        docker_login()

    archs = args.suffixes
    assert len(archs) > 1, "arch suffix input param is invalid"

    image_tags = (
        json.loads(args.image_tags)
        if not os.path.isfile(args.image_tags)
        else json.load(open(args.image_tags))
    )

    test_results = []
    status = "success"

    ok_cnt, fail_cnt = 0, 0
    images = get_images_oredered_list()
    for image_obj in images:
        tag = image_tags[image_obj.repo]
        if image_obj.only_amd64:
            # FIXME: WA until full arm support
            tags = [f"{tag}-{arch}" for arch in archs if arch != "aarch64"]
        else:
            tags = [f"{tag}-{arch}" for arch in archs]
        manifest, test_result = create_manifest(image_obj.repo, tag, tags, args.push)
        test_results.append(TestResult(manifest, test_result))
        if args.set_latest:
            manifest, test_result = create_manifest(
                image_obj.repo, "latest", tags, args.push
            )
            test_results.append(TestResult(manifest, test_result))

        if test_result != "OK":
            status = "failure"
            fail_cnt += 1
        else:
            ok_cnt += 1

    pr_info = PRInfo()
    s3_helper = S3Helper()

    url = upload_results(s3_helper, pr_info.number, pr_info.sha, test_results, [], NAME)

    print(f"::notice ::Report url: {url}")

    if not args.reports:
        return

    description = format_description(
        f"Multiarch images created [ok: {ok_cnt}, failed: {fail_cnt}]"
    )

    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)
    post_commit_status(
        commit, status, url, description, NAME, pr_info, dump_to_file=True
    )

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
    if status == "failure":
        sys.exit(1)


if __name__ == "__main__":
    main()
