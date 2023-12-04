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
from typing import Dict, Iterable, Set

from git_helper import git_runner
from version_helper import (
    ClickHouseVersion,
    get_supported_versions,
    get_tagged_versions,
    get_version_from_string,
)


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
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="set the script verbosity, could be used multiple",
    )
    parser.add_argument(
        "--directory",
        type=_rel_path,
        default=_rel_path("docker/official"),
        help="a relative to the reporitory root directory",
    )
    parser.add_argument(
        "--image-type",
        choices=["server", "keeper"],
        default="server",
        help="which image type to process",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="if set, the directory `docker/official` will be staged and committed "
        "after generating",
    )
    subparsers = parser.add_subparsers(
        title="commands", dest="command", required=True, help="the command to run"
    )
    parser_tree = subparsers.add_parser(
        "generate-tree", help="generates directory `docker/official`"
    )
    parser_tree.add_argument(
        "--min-version",
        type=get_version_from_string,
        default=None,
        help="if not set, only currently supported versions will be used",
    )
    parser_tree.add_argument(
        "--use-master-docker",
        action="store_true",
        help="by default, the `docker/server` from each branch is used to generate the "
        "directory; if this flag is set, then the `docker/server` from the "
        "`origin/master` branch is used",
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
    use_master_docker: bool,
    build_images: bool = False,
    image_type: str = "server",
) -> None:
    arg_version = "ARG VERSION="
    for version, directory in version_dirs.items():
        branch = "origin/master" if use_master_docker else version.describe
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
        version_dirs, args.use_master_docker, args.build_images, args.image_type
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


def main() -> None:
    args = parse_args()
    log_levels = [logging.CRITICAL, logging.WARN, logging.INFO, logging.DEBUG]
    logging.basicConfig(level=log_levels[min(args.verbose, 3)])
    logging.debug("Arguments are %s", pformat(args.__dict__))
    if args.command == "generate-tree":
        generate_tree(args)


if __name__ == "__main__":
    main()
