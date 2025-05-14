#!/usr/bin/env python3
import argparse
import concurrent.futures
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from commit_status_helper import format_description
from docker_images_helper import DockerImageData, docker_login, get_images_oredered_list
from env_helper import DOCKER_TAG, GITHUB_RUN_URL, RUNNER_TEMP
from report import FAILURE, SUCCESS, JobReport, StatusType, TestResult, TestResults
from stopwatch import Stopwatch
from tee_popen import TeePopen

TEMP_PATH = Path(RUNNER_TEMP) / "docker_images_check"
TEMP_PATH.mkdir(parents=True, exist_ok=True)


def check_missing_images_on_dockerhub(
    image_name_tag: Dict[str, str], arch: Optional[str] = None
) -> Dict[str, str]:
    """
    Checks missing images on dockerhub.
    Works concurrently for all given images.
    Docker must be logged in.
    """

    def run_docker_command(
        image: str, image_digest: str, arch: Optional[str] = None
    ) -> Dict:
        """
        aux command for fetching single docker manifest
        """
        command = [
            "docker",
            "manifest",
            "inspect",
            f"{image}:{image_digest}" if not arch else f"{image}:{image_digest}-{arch}",
        ]

        process = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )

        return {
            "image": image,
            "image_digest": image_digest,
            "arch": arch,
            "stdout": process.stdout,
            "stderr": process.stderr,
            "return_code": process.returncode,
        }

    result: Dict[str, str] = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(run_docker_command, image, tag, arch)
            for image, tag in image_name_tag.items()
        ]

        responses = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]
        for resp in responses:
            name, stdout, stderr, digest, arch = (
                resp["image"],
                resp["stdout"],
                resp["stderr"],
                resp["image_digest"],
                resp["arch"],
            )
            if stderr:
                if stderr.startswith("no such manifest"):
                    result[name] = digest
                else:
                    print(f"Error: Unknown error: {stderr}, {name}, {arch}")
            elif stdout:
                if "mediaType" in stdout:
                    pass
                else:
                    print(f"Error: Unknown response: {stdout}")
                    assert False, "FIXME"
            else:
                print(f"Error: No response for {name}, {digest}, {arch}")
                assert False, "FIXME"
    return result


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Program to build changed or given docker images with all "
        "dependant images. Example for local running: "
        "python docker_images_check.py --no-push-images "
        "--image-path docker/packager/binary",
    )

    parser.add_argument("--suffix", type=str, required=True, help="arch suffix")
    parser.add_argument(
        "--missing-images",
        type=str,
        default="",
        help="json string or json file with images to build {IMAGE: TAG} or type all to build all",
    )
    parser.add_argument(
        "--image-tags",
        type=str,
        default="",
        help="json string or json file with all images and their tags {IMAGE: TAG}",
    )
    parser.add_argument("--push", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-push",
        action="store_false",
        dest="push",
        default=argparse.SUPPRESS,
        help="don't push images to docker hub",
    )
    parser.add_argument(
        "--set-latest",
        action="store_true",
        help="add latest tag",
    )
    parser.add_argument(
        "--multiarch-manifest",
        action="store_true",
        help="create multi arch manifest",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)
    stopwatch = Stopwatch()

    args = parse_args()

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
        image_tags = (
            json.loads(args.image_tags) if args.image_tags else json.loads(DOCKER_TAG)
        )

    if args.missing_images == "all":
        missing_images = image_tags
    elif not args.missing_images:
        missing_images = check_missing_images_on_dockerhub(image_tags)
    elif os.path.isfile(args.missing_images):
        with open(args.missing_images, "r", encoding="utf-8") as jfd:
            missing_images = json.load(jfd)
    else:
        missing_images = json.loads(args.missing_images)

    print(f"Images to build: {missing_images}")
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

    if args.multiarch_manifest:
        print("Create multiarch manifests")
        images = get_images_oredered_list()
        for image_obj in images:
            if image_obj.repo not in missing_images and not args.set_latest:
                continue
            print(f"Create multiarch manifests for {image_obj.repo}")
            tag = image_tags[image_obj.repo]
            if image_obj.only_amd64:
                tags = [
                    f"{tag}-{arch}"
                    for arch in ("aarch64", "amd64")
                    if arch != "aarch64"
                ]
            else:
                tags = [f"{tag}-{arch}" for arch in ("aarch64", "amd64")]

            # 1. update multiarch latest manifest for every image
            manifest, test_result = create_manifest(
                image_obj.repo, tag, tags, args.push
            )
            test_results.append(TestResult(manifest, test_result))
            if args.set_latest:
                manifest, test_result = create_manifest(
                    image_obj.repo, "latest", tags, args.push
                )
                test_results.append(TestResult(manifest, test_result))
            if test_result != "OK":
                status = FAILURE
            else:
                ok_cnt += 1

    if status != SUCCESS:
        description = format_description(
            f"Images build done. built {ok_cnt} out of {len(missing_images)} images."
        )
    else:
        description = ""

    JobReport(
        description=description,
        test_results=test_results,
        status=status,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        additional_files=[],
    ).dump()

    if status == FAILURE:
        sys.exit(1)


if __name__ == "__main__":
    main()
