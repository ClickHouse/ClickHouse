import argparse
import atexit
import json
import logging
import os
import tempfile
import traceback
from pathlib import Path
from typing import Dict, List

from ci.jobs.scripts.clickhouse_version import CHVersion
from ci.praktika import Secret
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

ARCH = ("amd64", "arm64")

temp_path = Path(f"{Utils.cwd()}/ci/tmp")

GITHUB_SERVER_URL = os.getenv("GITHUB_SERVER_URL", "https://github.com")
with tempfile.NamedTemporaryFile("w", delete=False) as f:
    GIT_KNOWN_HOSTS_FILE = f.name
    GIT_PREFIX = (  # All commits to remote are done as robot-clickhouse
        "git -c user.email=robot-clickhouse@users.noreply.github.com "
        "-c user.name=robot-clickhouse -c commit.gpgsign=false "
        "-c core.sshCommand="
        f"'ssh -o UserKnownHostsFile={GIT_KNOWN_HOSTS_FILE} "
        "-o StrictHostKeyChecking=accept-new'"
    )
    atexit.register(os.remove, f.name)


def read_build_urls(build_name: str):
    artifact_report = temp_path / f"artifact_report_build_{build_name}.json"
    if artifact_report.is_file():
        with open(artifact_report, "r", encoding="utf-8") as f:
            return json.load(f)["build_urls"]
    return []


class DockerImageData:
    def __init__(self, name: str, path: str):
        self.name = name
        assert not path.startswith("/")
        self.path = path


class DelOS(argparse.Action):
    def __call__(self, _, namespace, __, option_string=None):
        no_build = self.dest[3:] if self.dest.startswith("no_") else self.dest
        if no_build in namespace.os:
            namespace.os.remove(no_build)


def docker_login(relogin: bool = True) -> None:
    if relogin or not Shell.check(
        "docker system info | grep --quiet -E 'Username|Registry'"
    ):
        Shell.check(
            "docker login --username 'robotclickhouse' --password-stdin",
            strict=True,
            stdin_str=Secret.Config(
                "dockerhub_robot_password", type=Secret.Type.AWS_SSM_PARAMETER
            ).get_value(),
            encoding="utf-8",
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="A program to build clickhouse-server image, both alpine and "
        "ubuntu versions",
    )
    parser.add_argument(
        "--tag-type",
        type=str,
        choices=("head", "release", "release-latest"),
        default="head",
        help="defines required tags for resulting docker image. "
        "head - for master image (tag: head) "
        "release - for release image (tags: XX, XX.XX, XX.XX.XX, XX.XX.XX.XX) "
        "release-latest - for latest release image (tags: XX, XX.XX, XX.XX.XX, XX.XX.XX.XX, latest) ",
    )
    parser.add_argument(
        "--image-path",
        type=str,
        default="",
        help="a path to docker context directory",
    )
    parser.add_argument(
        "--image-repo",
        type=str,
        default="",
        help="image name on docker hub",
    )
    parser.add_argument(
        "--bucket-prefix",
        help="if set, then is used as source for deb and tgz files",
    )
    parser.add_argument("--reports", default=True, help=argparse.SUPPRESS)
    parser.add_argument("--push", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--os", default=["ubuntu", "alpine"], help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-ubuntu",
        action=DelOS,
        nargs=0,
        default=argparse.SUPPRESS,
        help="don't build ubuntu image",
    )
    parser.add_argument(
        "--no-alpine",
        action=DelOS,
        nargs=0,
        default=argparse.SUPPRESS,
        help="don't build alpine image",
    )
    parser.add_argument(
        "--allow-build-reuse",
        action="store_true",
        help="allows binaries built on different branch if source digest matches current repo state",
    )

    return parser.parse_args()


def gen_tags(version_str: str, tag_type: str) -> List[str]:
    """
    @tag_type release-latest, @version 22.2.2.2:
    - latest
    - 22
    - 22.2
    - 22.2.2
    - 22.2.2.2
    @tag_type release, @version 22.2.2.2:
    - 22
    - 22.2
    - 22.2.2
    - 22.2.2.2
    @tag_type head:
    - head
    """
    parts = version_str.split(".")
    tags = []
    if tag_type == "release-latest":
        tags.append("latest")
        for i in range(len(parts)):
            tags.append(".".join(parts[: i + 1]))
    elif tag_type == "head":
        tags.append(tag_type)
    elif tag_type == "release":
        for i in range(len(parts)):
            tags.append(".".join(parts[: i + 1]))
    else:
        assert False, f"Invalid release type [{tag_type}]"
    return tags


def buildx_args(
    urls: Dict[str, str],
    arch: str,
    direct_urls: List[str],
    version: str,
    sha: str,
    action_url: str,
) -> List[str]:
    args = [
        f"--platform=linux/{arch}",
        f"--label=build-url={action_url}",
        f"--label=com.clickhouse.build.githash={sha}",
        f"--label=com.clickhouse.build.version={version}",
    ]
    if direct_urls:
        args.append(f"--build-arg=DIRECT_DOWNLOAD_URLS='{' '.join(direct_urls)}'")
    elif urls:
        url = urls[arch]
        args.append(f"--build-arg=REPOSITORY='{url}'")
        args.append(f"--build-arg=deb_location_url='{url}'")
    return args


def build_and_push_image(
    image: DockerImageData,
    push: bool,
    repo_urls: dict[str, str],
    os: str,
    tag: str,
    version: str,
    direct_urls: Dict[str, List[str]],
    run_url: str,
    sha: str,
) -> List[Result]:
    result = []
    if os != "ubuntu":
        tag += f"-{os}"
    init_args = ["docker", "buildx", "build"]
    if push:
        init_args.append("--push")
        init_args.append("--output=type=image,push-by-digest=true")
        init_args.append(f"--tag={image.name}")
    else:
        init_args.append("--output=type=docker")

    # `docker buildx build --load` does not support multiple images currently
    # images must be built separately and merged together with `docker manifest`
    digests = []

    for arch in ARCH:
        arch_tag = f"{tag}-{arch}"
        metadata_path = temp_path / arch_tag
        dockerfile = f"{image.path}/Dockerfile.{os}"
        cmd_args = list(init_args)
        urls = []
        if direct_urls:
            if os == "ubuntu" and "clickhouse-server" in image.name:
                urls = [url for url in direct_urls[arch] if ".deb" in url]
            else:
                urls = [url for url in direct_urls[arch] if ".tgz" in url]
        cmd_args.extend(
            buildx_args(
                repo_urls,
                arch,
                direct_urls=urls,
                version=version,
                action_url=run_url,
                sha=sha,
            )
        )
        if not push:
            cmd_args.append(f"--tag={image.name}:{arch_tag}")
        cmd_args.extend(
            [
                f"--metadata-file={metadata_path}",
                f"--build-arg=VERSION='{version}'",
                "--progress=plain",
                f"--file={dockerfile}",
                Path(image.path).as_posix(),
            ]
        )
        cmd = " ".join(cmd_args)
        logging.info("Building image %s:%s for arch %s: %s", image.name, tag, arch, cmd)
        result.append(
            Result.from_commands_run(name=f"{image.name}:{tag}-{arch}", command=cmd)
        )
        if not result[-1].is_ok():
            return result
        with open(metadata_path, "rb") as m:
            metadata = json.load(m)
            digests.append(metadata["containerimage.digest"])
    if push:
        cmd = (
            "docker buildx imagetools create "
            f"--tag {image.name}:{tag} {' '.join(digests)}"
        )
        logging.info("Pushing merged %s:%s image: %s", image.name, tag, cmd)
        result.append(Result.from_commands_run(name=f"{image.name}:{tag}", command=cmd))
        if not result[-1].is_ok():
            return result
    else:
        logging.info(
            "Merging is available only on push, separate %s images are created",
            f"{image.name}:{tag}-$arch",
        )

    return result


def test_docker_library(test_results) -> None:
    """we test our images vs the official docker library repository to track integrity"""
    arch = "amd64" if Utils.is_amd() else "arm64"
    check_images = [tr.name for tr in test_results if tr.name.endswith(f"-{arch}")]
    if not check_images:
        return
    test_name = "docker library image test"
    try:
        repo = "docker-library/official-images"
        logging.info("Cloning %s repository to run tests for 'clickhouse' image", repo)
        repo_path = temp_path / repo
        config_override = (
            Path(Utils.cwd()) / "ci/jobs/scripts/docker_server/config.sh"
        ).absolute()
        Shell.check(f"{GIT_PREFIX} clone {GITHUB_SERVER_URL}/{repo} {repo_path}")
        run_sh = (repo_path / "test/run.sh").absolute()
        for image in check_images:
            cmd = f"{run_sh} {image} -c {repo_path / 'test/config.sh'} -c {config_override}"
            test_results.append(
                Result.from_commands_run(name=f"{test_name} ({image})", command=cmd)
            )

    except Exception as e:
        logging.error("Failed while testing the docker library image: %s", e)
        test_results.append(
            Result(
                name=test_name,
                status=Result.Status.FAILED,
                info=f"Exception while testing docker library: {traceback.format_exc()}",
            )
        )


def main():
    logging.basicConfig(level=logging.INFO)
    sw = Utils.Stopwatch()
    os.makedirs(temp_path, exist_ok=True)

    args = parse_args()
    info = Info()

    version_dict = None
    if not info.is_local_run:
        version_dict = info.get_kv_data("version")
    if not version_dict:
        version_dict = CHVersion.get_current_version_as_dict()
        if not info.is_local_run:
            print(
                "WARNING: ClickHouse version has not been found in workflow kv storage - read from repo"
            )
            info.add_workflow_report_message(
                "WARNING: ClickHouse version has not been found in workflow kv storage"
            )
    assert version_dict

    if not info.is_local_run:
        assert not args.image_path and not args.image_repo

    if "server image" in info.job_name:
        image_path = args.image_path or "docker/server"
        image_repo = args.image_repo or "clickhouse/clickhouse-server"
    elif "keeper image" in info.job_name:
        image_path = args.image_path or "docker/keeper"
        image_repo = args.image_repo or "clickhouse/clickhouse-keeper"
    else:
        assert False, f"Unexpected job name [{info.job_name}]"

    push = args.push
    del args.image_path
    del args.image_repo
    del args.push

    if (
        info.is_push_event
        and info.git_branch == "master"
        and info.pr_number == 0
        and not info.is_local_run
    ):
        print("Set push flag for Master CI run")
        push = True

    image = DockerImageData(image_repo, image_path)
    tags = gen_tags(version_dict["string"], args.tag_type)
    repo_urls = {}
    direct_urls: Dict[str, List[str]] = {}

    for arch, build_name in zip(ARCH, ("amd_release", "arm_release")):
        if args.allow_build_reuse:
            # read s3 urls from pre-downloaded build reports
            if "clickhouse-server" in image_repo:
                PACKAGES = [
                    "clickhouse-client",
                    "clickhouse-server",
                    "clickhouse-common-static",
                ]
            elif "clickhouse-keeper" in image_repo:
                PACKAGES = ["clickhouse-keeper"]
            else:
                assert False, "BUG"
            urls = read_build_urls(build_name)
            assert urls, f"URLS has not been read from build report"
            direct_urls[arch] = [
                url
                for url in urls
                if any(package in url for package in PACKAGES) and "-dbg" not in url
            ]
        elif args.bucket_prefix:
            assert not args.allow_build_reuse
            repo_urls[arch] = f"{args.bucket_prefix}/{build_name}"
            print(f"Bucket prefix is set: Fetching packages from [{repo_urls}]")
        else:
            assert (
                False
            ), "--sha, --bucket_prefix or --allow-build-reuse (to fetch packages from build report) must be provided"

    if push:
        docker_login()

    logging.info("Following tags will be created: %s", ", ".join(tags))
    test_results = []
    for os_ in args.os:
        for tag in tags:
            test_results.extend(
                build_and_push_image(
                    image,
                    push,
                    repo_urls,
                    os_,
                    tag,
                    version_dict["describe"],
                    direct_urls,
                    run_url=info.run_url,
                    sha=info.sha,
                )
            )

    if not push:
        # The image is built locally only when we don't push it
        # See `--output=type=docker`
        test_docker_library(test_results)

    Result.create_from(results=test_results, stopwatch=sw).complete_job()


if __name__ == "__main__":
    main()
