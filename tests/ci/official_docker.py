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

from env_helper import GITHUB_REPOSITORY
from git_helper import Git, git_runner
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


DOCKER_LIBRARY_NAME = {"server": "clickhouse"}


def _rel_path(arg: str) -> Path:
    path = Path(arg)
    if path.is_absolute():
        raise argparse.ArgumentError(None, message=f"path '{path}' must be relative")
    return path


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
        type=_rel_path,
        default=_rel_path("docker/official"),
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
    docker_branch = "origin/master"
    parser_tree.add_argument(
        "--use-docker-from-branch",
        action="store_true",
        help="by default, the `docker/server` from each branch is used to generate the "
        "directory; if this flag is set, then the `docker/server` from the "
        "`--docker-branch` value is used",
    )
    parser_tree.add_argument(
        "--docker-branch",
        default=docker_branch,
        help="the branch to get the content of `docker/server` directory",
    )
    parser_tree.add_argument(
        "--build-images",
        action="store_true",
        help="if set, the image will be built for each Dockerfile.* in the result dirs",
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
    supported = {}  # type: Dict[str, ClickHouseVersion]
    versions = get_tagged_versions()
    for v in versions:
        if v < minimal:
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
    use_docker_from_branch: bool,
    docker_branch: str,
    build_images: bool = False,
    image_type: str = "server",
) -> None:
    arg_version = "ARG VERSION="
    for version, directory in version_dirs.items():
        branch = docker_branch if use_docker_from_branch else version.describe
        logging.debug(
            "Checkout directory content from '%s:docker/%s' to %s",
            branch,
            image_type,
            directory,
        )
        git_runner(
            f"git archive {branch}:docker/{image_type} | " f"tar x -C {directory}"
        )
        (directory / "README.md").unlink(missing_ok=True)
        for df in directory.glob("Dockerfile.*"):
            content = df.read_text().splitlines()
            for idx, line in enumerate(content):
                if line.startswith(arg_version):
                    logging.debug(
                        "Found '%s' in line %s:%s, setting to version %s",
                        arg_version,
                        df.name,
                        idx,
                        version,
                    )
                    content[idx] = f'{arg_version}"{version}"'
            df.write_text("\n".join(content) + "\n")
            if build_images:
                git_runner(
                    f"docker build --network=host -t '{DOCKER_LIBRARY_NAME[image_type]}:"
                    f"{version}{df.suffix}' -f '{df}' --progress plain '{directory}'"
                )


def path_is_changed(path: Path) -> bool:
    logging.info("Checking if the path %s is changed", path)
    return bool(git_runner(f"git status --porcelain -- '{path}'"))


def get_cmdline(width: int = 80) -> str:
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
        git_runner("git fetch --tags --no-recurse-submodules")
    if args.min_version:
        versions = get_versions_greater(args.min_version)
    else:
        versions = get_supported_versions()
    logging.info(
        "The versions to generate:\n  %s",
        "\n  ".join(v.string for v in sorted(versions)),
    )
    directory = Path(git_runner.cwd) / args.directory / args.image_type
    if args.clean:
        try:
            logging.info("Removing directory %s before generating", directory)
            rmtree(directory)
        except FileNotFoundError:
            pass
    version_dirs = create_versions_dirs(versions, directory)
    generate_docker_directories(
        version_dirs,
        args.use_docker_from_branch,
        args.docker_branch,
        args.build_images,
        args.image_type,
    )
    if args.commit and path_is_changed(directory):
        logging.info("Staging and committing content of %s", directory)
        git_runner(f"git add {directory}")
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
        git_runner("git commit -F -", input=commit_message)


@dataclass
class TagAttrs:
    """the metadata to preserve between generating tags for different versions"""

    # Only one latest can exist
    latest: ClickHouseVersion
    # Only one can be a major one (the most fresh per a year)
    majors: Dict[int, ClickHouseVersion]
    # Only one lts version can exist
    lts: Optional[ClickHouseVersion]


def ldf_header(git: Git, directory: Path) -> List[str]:
    script_path = Path(__file__).relative_to(git.root)
    script_sha = git_runner(f"git log -1 --format=format:%H -- {script_path}")
    repo = f"https://github.com/{GITHUB_REPOSITORY}"
    fetch_commit = git_runner(f"git log -1 --format=format:%H -- {directory}")
    return [
        f"# The file is generated by {repo}/blob/{script_sha}/{script_path}",
        "",
        "Maintainers: Misha f. Shiryaev <felixoid@clickhouse.com> (@Felixoid),",
        "             Max Kainov <max.kainov@clickhouse.com> (@mkaynov),",
        "             Alexander Sapin <alesapin@clickhouse.com> (@alesapin)",
        f"GitRepo: {repo}.git",
        f"GitFetch: refs/heads/{git.branch}",
        f"GitCommit: {fetch_commit}",
    ]


def ldf_tags(version: ClickHouseVersion, distro: str, tag_attrs: TagAttrs) -> str:
    """returns the string 'Tags: coma, separated, tags'"""
    tags = []
    without_distro = distro in UBUNTU_NAMES.values()
    with_lts = tag_attrs.lts in (None, version) and version.is_lts
    if version == tag_attrs.latest:
        if without_distro:
            tags.append("latest")
        tags.append(distro)

    if with_lts:
        tag_attrs.lts = version
        if without_distro:
            tags.append("lts")
        tags.append(f"{distro}-lts")

    with_major = tag_attrs.majors.get(version.major) in (None, version)
    if with_major:
        tag_attrs.majors[version.major] = version
        if without_distro:
            tags.append(f"{version.major}")
        tags.append(f"{version.major}-{distro}")

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
    "collect all Dockerfile.*"
    directory = Path(git_runner.cwd) / args.directory / args.image_type
    versions = sorted([get_version_from_string(d.name) for d in directory.iterdir()])
    assert versions
    if args.check_changed:
        assert not path_is_changed(directory)
    git = Git(True)
    lines = ldf_header(git, directory)
    tag_attrs = TagAttrs(versions[-1], {}, None)
    for version in reversed(versions):
        tag_dir = directory / str(version)
        for file in tag_dir.glob("Dockerfile.*"):
            lines.append("")
            distro = file.suffix[1:]
            if distro == "ubuntu":
                with open(file, "r", encoding="utf-8") as fd:
                    for l in fd:
                        if l.startswith("FROM ubuntu:"):
                            ubuntu_version = l.split(":")[-1].strip()
                            distro = UBUNTU_NAMES[ubuntu_version]
                            break
            lines.append(ldf_tags(version, distro, tag_attrs))
            lines.append("Architectures: amd64, arm64v8")
            lines.append(f"Directory: {tag_dir.relative_to(git.root)}")
            lines.append(f"File: {file.name}")

    lines.append("")
    ldf_file = (
        Path(git_runner.cwd) / args.directory / DOCKER_LIBRARY_NAME[args.image_type]
    )
    ldf_file.write_text("\n".join(lines))
    if args.commit and path_is_changed(ldf_file):
        git_runner(f"git add {ldf_file}")
        commit_message = (
            f"Re-/Generated docker LDF for {args.image_type} image\n\n"
            f"The file is generated and committed as following:\n{get_cmdline()}"
        )
        git_runner("git commit -F -", input=commit_message)
    print("\n".join(lines))


def main() -> None:
    args = parse_args()
    log_levels = [logging.CRITICAL, logging.WARN, logging.INFO, logging.DEBUG]
    logging.basicConfig(level=log_levels[min(args.verbose, 3)])
    logging.debug("Arguments are %s", pformat(args.__dict__))
    if args.command == "generate-tree":
        generate_tree(args)
    elif args.command == "generate-ldf":
        generate_ldf(args)


if __name__ == "__main__":
    main()
