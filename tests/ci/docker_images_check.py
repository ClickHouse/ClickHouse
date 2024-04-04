#!/usr/bin/env python3
import argparse
import json
import logging
import platform
import subprocess
import time
import sys
from pathlib import Path
from typing import Any, List, Optional, Set, Tuple, Union

from github import Github

from clickhouse_helper import ClickHouseHelper, prepare_tests_results_for_clickhouse
from commit_status_helper import format_description, get_commit, post_commit_status
from env_helper import REPO_COPY, RUNNER_TEMP, GITHUB_RUN_URL
from get_robot_token import get_best_robot_token, get_parameter_from_ssm
from pr_info import PRInfo
from report import TestResults, TestResult
from s3_helper import S3Helper
from stopwatch import Stopwatch
from tee_popen import TeePopen
from upload_result_helper import upload_results
from docker_images_helper import ImagesDict, IMAGES_FILE_PATH, get_images_dict

NAME = "Push to Dockerhub"
TEMP_PATH = Path(RUNNER_TEMP) / "docker_images_check"
TEMP_PATH.mkdir(parents=True, exist_ok=True)


class DockerImage:
    def __init__(
        self,
        path: str,
        repo: str,
        only_amd64: bool,
        parent: Optional["DockerImage"] = None,
        gh_repo: str = REPO_COPY,
    ):
        assert not path.startswith("/")
        self.path = path
        self.full_path = Path(gh_repo) / path
        self.repo = repo
        self.only_amd64 = only_amd64
        self.parent = parent
        self.built = False

    def __eq__(self, other) -> bool:  # type: ignore
        """Is used to check if DockerImage is in a set or not"""
        return (
            self.path == other.path
            and self.repo == self.repo
            and self.only_amd64 == other.only_amd64
        )

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, DockerImage):
            return False
        if self.parent and not other.parent:
            return False
        if not self.parent and other.parent:
            return True
        if self.path < other.path:
            return True
        if self.repo < other.repo:
            return True
        return False

    def __hash__(self):
        return hash(self.path)

    def __str__(self):
        return self.repo

    def __repr__(self):
        return f"DockerImage(path={self.path},repo={self.repo},parent={self.parent})"


def get_changed_docker_images(
    pr_info: PRInfo, images_dict: ImagesDict
) -> Set[DockerImage]:
    if not images_dict:
        return set()

    files_changed = pr_info.changed_files

    logging.info(
        "Changed files for PR %s @ %s: %s",
        pr_info.number,
        pr_info.sha,
        str(files_changed),
    )

    changed_images = []

    for dockerfile_dir, image_description in images_dict.items():
        for f in files_changed:
            if f.startswith(dockerfile_dir):
                name = image_description["name"]
                only_amd64 = image_description.get("only_amd64", False)
                logging.info(
                    "Found changed file '%s' which affects "
                    "docker image '%s' with path '%s'",
                    f,
                    name,
                    dockerfile_dir,
                )
                changed_images.append(DockerImage(dockerfile_dir, name, only_amd64))
                break

    # The order is important: dependents should go later than bases, so that
    # they are built with updated base versions.
    index = 0
    while index < len(changed_images):
        image = changed_images[index]
        for dependent in images_dict[image.path]["dependent"]:
            logging.info(
                "Marking docker image '%s' as changed because it "
                "depends on changed docker image '%s'",
                dependent,
                image,
            )
            name = images_dict[dependent]["name"]
            only_amd64 = images_dict[dependent].get("only_amd64", False)
            changed_images.append(DockerImage(dependent, name, only_amd64, image))
        index += 1
        if index > 5 * len(images_dict):
            # Sanity check to prevent infinite loop.
            raise RuntimeError(
                f"Too many changed docker images, this is a bug. {changed_images}"
            )

    # With reversed changed_images set will use images with parents first, and
    # images without parents then
    result = set(reversed(changed_images))
    logging.info(
        "Changed docker images for PR %s @ %s: '%s'",
        pr_info.number,
        pr_info.sha,
        result,
    )
    return result


def gen_versions(
    pr_info: PRInfo, suffix: Optional[str]
) -> Tuple[List[str], Union[str, List[str]]]:
    pr_commit_version = str(pr_info.number) + "-" + pr_info.sha
    # The order is important, PR number is used as cache during the build
    versions = [str(pr_info.number), pr_commit_version]
    result_version = pr_commit_version  # type: Union[str, List[str]]
    if pr_info.number == 0 and pr_info.base_ref == "master":
        # First get the latest for cache
        versions.insert(0, "latest")

    if suffix:
        # We should build architecture specific images separately and merge a
        # manifest lately in a different script
        versions = [f"{v}-{suffix}" for v in versions]
        # changed_images_{suffix}.json should contain all changed images
        result_version = versions

    return versions, result_version


def build_and_push_dummy_image(
    image: DockerImage,
    version_string: str,
    push: bool,
) -> Tuple[bool, Path]:
    dummy_source = "ubuntu:20.04"
    logging.info("Building docker image %s as %s", image.repo, dummy_source)
    build_log = (
        Path(TEMP_PATH)
        / f"build_and_push_log_{image.repo.replace('/', '_')}_{version_string}.log"
    )
    cmd = (
        f"docker pull {dummy_source}; "
        f"docker tag {dummy_source} {image.repo}:{version_string}; "
    )
    if push:
        cmd += f"docker push {image.repo}:{version_string}"

    logging.info("Docker command to run: %s", cmd)
    with TeePopen(cmd, build_log) as proc:
        retcode = proc.wait()

    if retcode != 0:
        return False, build_log

    logging.info("Processing of %s successfully finished", image.repo)
    return True, build_log


def build_and_push_one_image(
    image: DockerImage,
    version_string: str,
    additional_cache: List[str],
    push: bool,
    child: bool,
) -> Tuple[bool, Path]:
    if image.only_amd64 and platform.machine() not in ["amd64", "x86_64"]:
        return build_and_push_dummy_image(image, version_string, push)
    logging.info(
        "Building docker image %s with version %s from path %s",
        image.repo,
        version_string,
        image.full_path,
    )
    build_log = (
        Path(TEMP_PATH)
        / f"build_and_push_log_{image.repo.replace('/', '_')}_{version_string}.log"
    )
    push_arg = ""
    if push:
        push_arg = "--push "

    from_tag_arg = ""
    if child:
        from_tag_arg = f"--build-arg FROM_TAG={version_string} "

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
        f"--progress plain {image.full_path}"
    )
    logging.info("Docker command to run: %s", cmd)
    with TeePopen(cmd, build_log) as proc:
        retcode = proc.wait()

    if retcode != 0:
        return False, build_log

    logging.info("Processing of %s successfully finished", image.repo)
    return True, build_log


def process_single_image(
    image: DockerImage,
    versions: List[str],
    additional_cache: List[str],
    push: bool,
    child: bool,
) -> TestResults:
    logging.info("Image will be pushed with versions %s", ", ".join(versions))
    results = []  # type: TestResults
    for ver in versions:
        stopwatch = Stopwatch()
        for i in range(5):
            success, build_log = build_and_push_one_image(
                image, ver, additional_cache, push, child
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


def process_image_with_parents(
    image: DockerImage,
    versions: List[str],
    additional_cache: List[str],
    push: bool,
    child: bool = False,
) -> TestResults:
    results = []  # type: TestResults
    if image.built:
        return results

    if image.parent is not None:
        results += process_image_with_parents(
            image.parent, versions, additional_cache, push, False
        )
        child = True

    results += process_single_image(image, versions, additional_cache, push, child)
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Program to build changed or given docker images with all "
        "dependant images. Example for local running: "
        "python docker_images_check.py --no-push-images --no-reports "
        "--image-path docker/packager/binary",
    )

    parser.add_argument(
        "--suffix",
        type=str,
        help="suffix for all built images tags and resulting json file; the parameter "
        "significantly changes the script behavior, e.g. changed_images.json is called "
        "changed_images_{suffix}.json and contains list of all tags",
    )
    parser.add_argument(
        "--repo",
        type=str,
        default="clickhouse",
        help="docker hub repository prefix",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="rebuild all images",
    )
    parser.add_argument(
        "--image-path",
        type=str,
        nargs="*",
        help="list of image paths to build instead of using pr_info + diff URL, "
        "e.g. 'docker/packager/binary'",
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
    if args.suffix:
        global NAME
        NAME += f" {args.suffix}"
        changed_json = TEMP_PATH / f"changed_images_{args.suffix}.json"
    else:
        changed_json = TEMP_PATH / "changed_images.json"

    if args.push:
        subprocess.check_output(  # pylint: disable=unexpected-keyword-arg
            "docker login --username 'robotclickhouse' --password-stdin",
            input=get_parameter_from_ssm("dockerhub_robot_password"),
            encoding="utf-8",
            shell=True,
        )

    images_dict = get_images_dict(Path(REPO_COPY), IMAGES_FILE_PATH)

    pr_info = PRInfo()
    if args.all:
        pr_info.changed_files = set(images_dict.keys())
    elif args.image_path:
        pr_info.changed_files = set(i for i in args.image_path)
    else:
        try:
            pr_info.fetch_changed_files()
        except TypeError:
            # If the event does not contain diff, nothing will be built
            pass

    changed_images = get_changed_docker_images(pr_info, images_dict)
    if changed_images:
        logging.info(
            "Has changed images: %s", ", ".join([im.path for im in changed_images])
        )

    image_versions, result_version = gen_versions(pr_info, args.suffix)

    result_images = {}
    test_results = []  # type: TestResults
    additional_cache = []  # type: List[str]
    if pr_info.release_pr:
        logging.info("Use %s as additional cache tag", pr_info.release_pr)
        additional_cache.append(str(pr_info.release_pr))
    if pr_info.merged_pr:
        logging.info("Use %s as additional cache tag", pr_info.merged_pr)
        additional_cache.append(str(pr_info.merged_pr))

    for image in changed_images:
        # If we are in backport PR, then pr_info.release_pr is defined
        # We use it as tag to reduce rebuilding time
        test_results += process_image_with_parents(
            image, image_versions, additional_cache, args.push
        )
        result_images[image.repo] = result_version

    if changed_images:
        description = "Updated " + ",".join([im.repo for im in changed_images])
    else:
        description = "Nothing to update"

    description = format_description(description)

    with open(changed_json, "w", encoding="utf-8") as images_file:
        logging.info("Saving changed images file %s", changed_json)
        json.dump(result_images, images_file)

    s3_helper = S3Helper()

    status = "success"
    if [r for r in test_results if r.status != "OK"]:
        status = "failure"

    url = upload_results(s3_helper, pr_info.number, pr_info.sha, test_results, [], NAME)

    print(f"::notice ::Report url: {url}")

    if not args.reports:
        return

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

    if status == "failure":
        sys.exit(1)


if __name__ == "__main__":
    main()
