#!/usr/bin/env python
"""Plan:
- Create a PR with regenerated Dockerfiles for each release branch
- Generate `library definition file` as described in
https://github.com/docker-library/official-images/blob/master/README.md#library-definition-files
- Create a PR with it to https://github.com/docker-library/official-images, the file
name will be `library/clickhouse`"""

import argparse
import logging
from pathlib import Path
from pprint import pformat
from shutil import rmtree
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
    subparsers = parser.add_subparsers(
        title="commands", dest="command", help="the command to run"
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
        "directory; if this flag is set, then the `docker/server` from the `master` "
        "branch is used",
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


def generate_tree(args: argparse.Namespace) -> None:
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


def main() -> None:
    args = parse_args()
    log_levels = [logging.CRITICAL, logging.WARN, logging.INFO, logging.DEBUG]
    logging.basicConfig(level=log_levels[min(args.verbose, 3)])
    logging.debug("Arguments are %s", pformat(args.__dict__))
    if args.command == "generate-tree":
        generate_tree(args)


if __name__ == "__main__":
    main()
