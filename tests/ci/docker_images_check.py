#!/usr/bin/env python3
import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple

from github import Github

from clickhouse_helper import ClickHouseHelper, prepare_tests_results_for_clickhouse
from commit_status_helper import format_description, get_commit, post_commit_status
from docker_images_helper import DockerImageData, docker_login, get_images_oredered_list
from env_helper import GITHUB_RUN_URL, RUNNER_TEMP
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from report import FAILURE, SUCCESS, StatusType, TestResult, TestResults
from s3_helper import S3Helper
from stopwatch import Stopwatch
from tee_popen import TeePopen
from upload_result_helper import upload_results

TEMP_PATH = Path(RUNNER_TEMP) / "docker_images_check"
TEMP_PATH.mkdir(parents=True, exist_ok=True)


def build_and_push_one_image(
    image: DockerImageData,
    version_string: str,
    additional_cache: List[str],
    push: bool,
    from_tag: Optional[str] = None,
) -> Tuple[bool, Path]:
    logging.info(
        "Building docker image %s with version %s from path %s",
        image.repo,
        version_string,
        image.path,
    )
    build_log = (
        Path(TEMP_PATH)
        / f"build_and_push_log_{image.repo.replace('/', '_')}_{version_string}.log"
    )
    push_arg = ""
    if push:
        push_arg = "--push "

    from_tag_arg = ""
    if from_tag:
        from_tag_arg = f"--build-arg FROM_TAG={from_tag} "

    cache_from = (
        f"--cache-from type=registry,ref={image.repo}:{version_string} "
        f"--cache-from type=registry,ref={image.repo}:latest"
    )
    for tag in additional_cache:
        assert tag
        cache_from = f"{cache_from} --cache-from type=registry,ref={image.repo}:{tag}"

    cmd = (
        "docker buildx build --builder default "
        f"--label build-url={GITHUB_RUN_URL} "
        f"{from_tag_arg}"
        # A hack to invalidate cache, grep for it in docker/ dir
        f"--build-arg CACHE_INVALIDATOR={GITHUB_RUN_URL} "
        f"--tag {image.repo}:{version_string} "
        f"{cache_from} "
        f"--cache-to type=inline,mode=max "
        f"{push_arg}"
        f"--progress plain {image.path}"
    )
    logging.info("Docker command to run: %s", cmd)
    with TeePopen(cmd, build_log) as proc:
        retcode = proc.wait()

    if retcode != 0:
        return False, build_log

    logging.info("Processing of %s successfully finished", image.repo)
    return True, build_log


def process_single_image(
    image: DockerImageData,
    versions: List[str],
    additional_cache: List[str],
    push: bool,
    from_tag: Optional[str] = None,
) -> TestResults:
    logging.info("Image will be pushed with versions %s", ", ".join(versions))
    results = []  # type: TestResults
    for ver in versions:
        stopwatch = Stopwatch()
        for i in range(2):
            success, build_log = build_and_push_one_image(
                image, ver, additional_cache, push, from_tag
            )
            if success:
                results.append(
                    TestResult(
                        image.repo + ":" + ver,
                        "OK",
                        stopwatch.duration_seconds,
                        [build_log],
                    )
                )
                break
            logging.info(
                "Got error will retry %s time and sleep for %s seconds", i, i * 5
            )
            time.sleep(i * 5)
        else:
            results.append(
                TestResult(
                    image.repo + ":" + ver,
                    "FAIL",
                    stopwatch.duration_seconds,
                    [build_log],
                )
            )

    logging.info("Processing finished")
    image.built = True
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Program to build changed or given docker images with all "
        "dependant images. Example for local running: "
        "python docker_images_check.py --no-push-images --no-reports "
        "--image-path docker/packager/binary",
    )

    parser.add_argument("--suffix", type=str, required=True, help="arch suffix")
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

    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)
    stopwatch = Stopwatch()

    args = parse_args()

    NAME = f"Push to Dockerhub {args.suffix}"

    if args.push:
        logging.info("login to docker hub")
        docker_login()

    test_results = []  # type: TestResults
    additional_cache = []  # type: List[str]
    # FIXME: add all tags taht we need. latest on master!
    # if pr_info.release_pr:
    #     logging.info("Use %s as additional cache tag", pr_info.release_pr)
    #     additional_cache.append(str(pr_info.release_pr))
    # if pr_info.merged_pr:
    #     logging.info("Use %s as additional cache tag", pr_info.merged_pr)
    #     additional_cache.append(str(pr_info.merged_pr))

    ok_cnt = 0
    status = SUCCESS  # type: StatusType

    if os.path.isfile(args.image_tags):
        with open(args.image_tags, "r", encoding="utf-8") as jfd:
            image_tags = json.load(jfd)
    else:
        image_tags = json.loads(args.image_tags)

    if args.missing_images == "all":
        missing_images = image_tags
    elif os.path.isfile(args.missing_images):
        with open(args.missing_images, "r", encoding="utf-8") as jfd:
            missing_images = json.load(jfd)
    else:
        missing_images = json.loads(args.missing_images)

    images_build_list = get_images_oredered_list()

    for image in images_build_list:
        if image.repo not in missing_images:
            continue
        logging.info("Start building image: %s", image)

        image_versions = (
            [image_tags[image.repo]]
            if not args.suffix
            else [f"{image_tags[image.repo]}-{args.suffix}"]
        )
        parent_version = (
            None
            if not image.parent
            else (
                image_tags[image.parent]
                if not args.suffix
                else f"{image_tags[image.parent]}-{args.suffix}"
            )
        )

        res = process_single_image(
            image,
            image_versions,
            additional_cache,
            args.push,
            from_tag=parent_version,
        )
        test_results += res
        if all(x.status == "OK" for x in res):
            ok_cnt += 1
        else:
            status = FAILURE
            break  # No need to continue with next images

    description = format_description(
        f"Images build done. built {ok_cnt} out of {len(missing_images)} images."
    )

    s3_helper = S3Helper()

    pr_info = PRInfo()
    url = upload_results(s3_helper, pr_info.number, pr_info.sha, test_results, [], NAME)

    print(f"::notice ::Report url: {url}")

    if not args.reports:
        return

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

    if status == FAILURE:
        sys.exit(1)


if __name__ == "__main__":
    main()
