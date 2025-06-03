#!/usr/bin/env python
"""Plan:
- Create a PR with regenerated Dockerfiles for each release branch
- Generate `library definition file` as described in
https://github.com/docker-library/official-images/blob/master/README.md#library-definition-files
- Create a PR with it to https://github.com/docker-library/official-images, the file
name will be `library/clickhouse`"""

import argparse
import logging
from dataclasses import dataclass
from os import getpid
from pathlib import Path
from pprint import pformat
from shlex import quote
from shutil import rmtree
from textwrap import fill
from typing import Dict, Iterable, List, Optional, Set

from env_helper import GITHUB_REPOSITORY, IS_CI
from git_helper import GIT_PREFIX, Git, git_runner, is_shallow
from version_helper import (
    ClickHouseVersion,
    get_supported_versions,
    get_tagged_versions,
    get_version_from_string,
)

UBUNTU_NAMES = {
    "20.04": "focal",
    "22.04": "jammy",
}

if not IS_CI:
    GIT_PREFIX = "git"

DOCKER_LIBRARY_REPOSITORY = "ClickHouse/docker-library"

DOCKER_LIBRARY_NAME = {"server": "clickhouse"}

MAINTAINERS_HEADER = (
    "Maintainers: Misha f. Shiryaev <felixoid@clickhouse.com> (@Felixoid),\n"
    "             Max Kainov <max.kainov@clickhouse.com> (@mkaynov),\n"
    "             Alexander Sapin <alesapin@clickhouse.com> (@alesapin)"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="The script to handle tasks for docker-library/official-images",
    )
    global_args = argparse.ArgumentParser(add_help=False)
    global_args.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="set the script verbosity, could be used multiple",
    )
    global_args.add_argument(
        "--directory",
        type=Path,
        default=Path("docker/official"),
        help="a relative to the reporitory root directory",
    )
    global_args.add_argument(
        "--image-type",
        choices=["server", "keeper"],
        default="server",
        help="which image type to process",
    )
    global_args.add_argument(
        "--commit",
        action="store_true",
        help="if set, the directory `docker/official` will be staged and committed "
        "after generating",
    )
    dockerfile_glob = "Dockerfile.*"
    global_args.add_argument(
        "--dockerfile-glob",
        default=dockerfile_glob,
        help="a glob to collect Dockerfiles in the server of keeper directory",
    )
    subparsers = parser.add_subparsers(
        title="commands", dest="command", required=True, help="the command to run"
    )
    parser_tree = subparsers.add_parser(
        "generate-tree",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="generates directory `docker/official`",
        parents=[global_args],
    )
    parser_tree.add_argument(
        "--min-version",
        type=get_version_from_string,
        default=None,
        help="if not set, only currently supported versions will be used",
    )
    parser_tree.add_argument(
        "--docker-branch",
        default="",
        help="if set, the branch to get the content of `docker/server` directory. When "
        "unset, the content of directories is taken from release tags",
    )
    parser_tree.add_argument(
        "--build-images",
        action="store_true",
        help="if set, the image will be built for each Dockerfile '--dockerfile-glob' "
        "in the result dirs",
    )
    parser_tree.add_argument("--clean", default=True, help=argparse.SUPPRESS)
    parser_tree.add_argument(
        "--no-clean",
        dest="clean",
        action="store_false",
        default=argparse.SUPPRESS,
        help="if set, the directory `docker/official` won't be cleaned "
        "before generating",
    )
    parser_tree.add_argument(
        "--fetch-tags",
        action="store_true",
        help="if set, the tags will be updated before run",
    )
    parser_ldf = subparsers.add_parser(
        "generate-ldf",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="generate docker library definition file",
        parents=[global_args],
    )
    parser_ldf.add_argument("--check-changed", default=True, help=argparse.SUPPRESS)
    parser_ldf.add_argument(
        "--no-check-changed",
        dest="check_changed",
        action="store_false",
        default=argparse.SUPPRESS,
        help="if set, the directory `docker/official` won't be checked to be "
        "uncommitted",
    )
    args = parser.parse_args()
    return args


def get_versions_greater(minimal: ClickHouseVersion) -> Set[ClickHouseVersion]:
    "Get the latest patch version for each major.minor >= minimal"
    supported = {}  # type: Dict[str, ClickHouseVersion]
    versions = get_tagged_versions()
    for v in versions:
        if v < minimal or not v.is_supported:
            continue
        txt = f"{v.major}.{v.minor}"
        if txt in supported:
            supported[txt] = max(v, supported[txt])
            continue
        supported[txt] = v
    return set(supported.values())


def create_versions_dirs(
    versions: Iterable[ClickHouseVersion], directory: Path
) -> Dict[ClickHouseVersion, Path]:
    assert directory.is_dir() or not directory.exists()
    dirs = {}
    for v in versions:
        version_dir = directory / str(v)
        version_dir.mkdir(parents=True, exist_ok=True)
        logging.debug("Directory %s for version %s is created", version_dir, v)
        dirs[v] = version_dir
    return dirs


def generate_docker_directories(
    version_dirs: Dict[ClickHouseVersion, Path],
    docker_branch: str,
    dockerfile_glob: str,
    build_images: bool = False,
    image_type: str = "server",
) -> None:
    arg_version = "ARG VERSION="
    start_filter = "#docker-official-library:off"
    stop_filter = "#docker-official-library:on"
    for version, directory in version_dirs.items():
        branch = docker_branch or version.describe
        docker_source = f"{branch}:docker/{image_type}"
        logging.debug(
            "Unpack directory content from '%s' to %s",
            docker_source,
            directory,
        )
        # We ignore README* files
        git_runner(
            f"git archive {docker_source} | tar --exclude='README*' -x -C {directory}"
        )
        for df in directory.glob(dockerfile_glob):
            original_content = df.read_text().splitlines()
            content = []
            filtering = False
            for line in original_content:
                # Change the ARG VERSION= to a proper version
                if line.startswith(arg_version):
                    logging.debug(
                        "Found '%s' in line %s:%s, setting to version %s",
                        arg_version,
                        df.name,
                        original_content.index(line),
                        version,
                    )
                    content.append(f'{arg_version}"{version}"')
                    continue
                # Filter out CI-related part from official docker
                if line == start_filter:
                    filtering = True
                    continue
                if line == stop_filter:
                    filtering = False
                    continue
                if not filtering:
                    content.append(line)

            df.write_text("\n".join(content) + "\n")
            if build_images:
                git_runner(
                    f"docker build --network=host -t '{DOCKER_LIBRARY_NAME[image_type]}:"
                    f"{version}{df.suffix}' -f '{df}' --progress plain '{directory}'"
                )


def path_is_changed(path: Path) -> bool:
    "checks if `path` has uncommitted changes"
    logging.info("Checking if the path %s is changed", path)
    path_dir = path.parent
    return bool(git_runner(f"git -C {path_dir} status --porcelain -- '{path}'"))


def get_cmdline(width: int = 80) -> str:
    "Returns the cmdline split by words with given maximum width"
    cmdline = " ".join(
        quote(arg)
        for arg in Path(f"/proc/{getpid()}/cmdline")
        .read_text(encoding="utf-8")
        .split("\x00")[:-1]
    )
    if width <= 2:
        return cmdline

    return " \\\n".join(
        fill(
            cmdline,
            break_long_words=False,
            break_on_hyphens=False,
            subsequent_indent=r"  ",
            width=width - 2,
        ).split("\n")
    )


def generate_tree(args: argparse.Namespace) -> None:
    if args.fetch_tags:
        # Fetch all tags to not miss the latest versions
        git_runner(f"{GIT_PREFIX} fetch --tags --no-recurse-submodules")
    if args.min_version:
        versions = get_versions_greater(args.min_version)
    else:
        versions = get_supported_versions()
    logging.info(
        "The versions to generate:\n  %s",
        "\n  ".join(v.string for v in sorted(versions)),
    )
    directory = (args.directory / args.image_type).resolve()  # type: Path
    if args.clean:
        try:
            logging.info("Removing directory %s before generating", directory)
            rmtree(directory)
        except FileNotFoundError:
            pass
    version_dirs = create_versions_dirs(versions, directory)
    generate_docker_directories(
        version_dirs,
        args.docker_branch,
        args.dockerfile_glob,
        args.build_images,
        args.image_type,
    )
    if args.commit and path_is_changed(directory):
        logging.info("Staging and committing content of %s", directory)
        git_runner(f"git -C {directory} add {directory}")
        commit_message = "\n".join(
            (
                "Re-/Generated tags for official docker library",
                "",
                "The changed versions:",
                *(f"  {v.string}" for v in sorted(versions)),
                f"The old images were removed: {args.clean}",
                "The directory is generated and committed as following:",
                f"{get_cmdline()}",
            )
        )
        git_runner(f"{GIT_PREFIX} -C {directory} commit -F -", input=commit_message)


@dataclass
class TagAttrs:
    """A mutable metadata to preserve between generating tags for different versions"""

    # Only one latest can exist
    latest: ClickHouseVersion
    # Only one lts version can exist
    lts: Optional[ClickHouseVersion]


def ldf_header(git: Git, directory: Path) -> List[str]:
    "Generates the header for LDF"
    script_path = Path(__file__).relative_to(git.root)
    script_sha = git_runner(f"git log -1 --format=format:%H -- {script_path}")
    repo = f"https://github.com/{GITHUB_REPOSITORY}"
    dl_repo = f"https://github.com/{DOCKER_LIBRARY_REPOSITORY}"
    fetch_commit = git_runner(
        f"git -C {directory} log -1 --format=format:%H -- {directory}"
    )
    dl_branch = git_runner(f"git -C {directory} branch --show-current")
    return [
        f"# The file is generated by {repo}/blob/{script_sha}/{script_path}",
        "",
        MAINTAINERS_HEADER,
        f"GitRepo: {dl_repo}.git",
        f"GitFetch: refs/heads/{dl_branch}",
        f"GitCommit: {fetch_commit}",
    ]


def ldf_tags(version: ClickHouseVersion, distro: str, tag_attrs: TagAttrs) -> str:
    """returns the string 'Tags: coma, separated, tags'"""
    tags = []
    # without_distro shows that it's the default tags set, without `-jammy` suffix
    without_distro = distro in UBUNTU_NAMES.values()
    if version == tag_attrs.latest:
        if without_distro:
            tags.append("latest")
        tags.append(distro)

    # The current version gets the `lts` tag when it's the first met version.is_lts
    with_lts = tag_attrs.lts in (None, version) and version.is_lts
    if with_lts:
        tag_attrs.lts = version
        if without_distro:
            tags.append("lts")
        tags.append(f"lts-{distro}")

    # Add all normal tags
    for tag in (
        f"{version.major}.{version.minor}",
        f"{version.major}.{version.minor}.{version.patch}",
        f"{version.major}.{version.minor}.{version.patch}.{version.tweak}",
    ):
        if without_distro:
            tags.append(tag)
        tags.append(f"{tag}-{distro}")

    return f"Tags: {', '.join(tags)}"


def generate_ldf(args: argparse.Namespace) -> None:
    """Collect all args.dockerfile_glob files from args.directory and generate the
    Library Definition File, read about it in
    https://github.com/docker-library/official-images/?tab=readme-ov-file#library-definition-files
    """
    directory = (args.directory / args.image_type).resolve()  # type: Path
    versions = sorted([get_version_from_string(d.name) for d in directory.iterdir()])
    assert versions, "There are no directories to generate the LDF"
    if args.check_changed:
        assert not path_is_changed(
            directory
        ), f"There are uncommitted changes in {directory}"
    git = Git(True)
    # Support a few repositories, get the git-root for images directory
    dir_git_root = (
        directory / git_runner(f"git -C {directory} rev-parse --show-cdup")
    ).resolve()
    lines = ldf_header(git, directory)
    tag_attrs = TagAttrs(versions[-1], None)

    # We iterate from the most recent to the oldest version
    for version in reversed(versions):
        tag_dir = directory / str(version)
        for file in tag_dir.glob(args.dockerfile_glob):
            lines.append("")
            distro = file.suffix[1:]
            if distro == "ubuntu":
                # replace 'ubuntu' by the release name from UBUNTU_NAMES
                with open(file, "r", encoding="utf-8") as fd:
                    for l in fd:
                        if l.startswith("FROM ubuntu:"):
                            ubuntu_version = l.split(":")[-1].strip()
                            distro = UBUNTU_NAMES[ubuntu_version]
                            break
            lines.append(ldf_tags(version, distro, tag_attrs))
            lines.append("Architectures: amd64, arm64v8")
            lines.append(f"Directory: {tag_dir.relative_to(dir_git_root)}")
            lines.append(f"File: {file.name}")

    # For the last '\n' in join
    lines.append("")
    ldf_file = args.directory.resolve() / DOCKER_LIBRARY_NAME[args.image_type]
    ldf_file.write_text("\n".join(lines))
    logging.info("The content of LDF file:\n%s", "\n".join(lines))
    if args.commit and path_is_changed(ldf_file):
        logging.info("Starting committing or %s file", ldf_file)
        ldf_dir = ldf_file.parent
        git_runner(f"git -C {ldf_dir} add {ldf_file}")
        commit_message = (
            f"Re-/Generated docker LDF for {args.image_type} image\n\n"
            f"The file is generated and committed as following:\n{get_cmdline()}"
        )
        git_runner(f"{GIT_PREFIX} -C {ldf_dir} commit -F -", input=commit_message)


def main() -> None:
    args = parse_args()
    log_levels = [logging.CRITICAL, logging.WARN, logging.INFO, logging.DEBUG]
    logging.basicConfig(level=log_levels[min(args.verbose, 3)])
    logging.debug("Arguments are %s", pformat(args.__dict__))
    assert not is_shallow(), "The repository must be full for script to work"
    if args.command == "generate-tree":
        generate_tree(args)
    elif args.command == "generate-ldf":
        generate_ldf(args)


if __name__ == "__main__":
    main()
