#!/usr/bin/env python3

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from ci_utils import Shell
from env_helper import DOCKER_TAG, ROOT_DIR
from get_robot_token import get_parameter_from_ssm

IMAGES_FILE_PATH = Path("docker/images.json")

ImagesDict = Dict[str, dict]


def docker_login(relogin: bool = True) -> None:
    if relogin or not Shell.check(
        "docker system info | grep --quiet -E 'Username|Registry'"
    ):
        Shell.check(  # pylint: disable=unexpected-keyword-arg
            "docker login --username 'robotclickhouse' --password-stdin",
            strict=True,
            stdin_str=get_parameter_from_ssm("dockerhub_robot_password"),
            encoding="utf-8",
        )


class DockerImage:
    def __init__(self, name: str, version: Optional[str] = None):
        self.name = name
        if version is None:
            self.version = "latest"
        else:
            self.version = version

    def __str__(self):
        return f"{self.name}:{self.version}"


def pull_image(image: DockerImage) -> DockerImage:
    try:
        logging.info("Pulling image %s - start", image)
        Shell.check(f"docker pull {image}", strict=True)
        logging.info("Pulling image %s - done", image)
    except Exception as ex:
        logging.info("Got exception pulling docker %s", ex)
        raise ex
    return image


def get_docker_image(image_name: str) -> DockerImage:
    assert DOCKER_TAG and isinstance(DOCKER_TAG, str), "DOCKER_TAG env must be provided"
    if "{" in DOCKER_TAG:
        tags_map = json.loads(DOCKER_TAG)
        assert (
            image_name in tags_map
        ), "Image name does not exist in provided DOCKER_TAG json string"
        return DockerImage(image_name, tags_map[image_name])
    # DOCKER_TAG is a tag itself
    return DockerImage(image_name, DOCKER_TAG)


class DockerImageData:
    def __init__(
        self,
        path: str,
        repo: str,
        only_amd64: bool,
        parent: Optional["DockerImageData"] = None,
    ):
        assert not path.startswith("/")
        self.path = Path(ROOT_DIR) / path
        self.repo = repo
        self.only_amd64 = only_amd64
        self.parent = parent
        self.built = False

    def __eq__(self, other) -> bool:  # type: ignore
        """Is used to check if DockerImageData is in a set or not"""
        return (
            self.path == other.path
            and self.repo == self.repo
            and self.only_amd64 == other.only_amd64
        )

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, DockerImageData):
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
        return (
            f"DockerImageData(path={self.path},repo={self.repo},parent={self.parent})"
        )


def get_images_dict(
    repo_path: Optional[Path] = None, images_file_path: Optional[Path] = None
) -> ImagesDict:
    """Return images suppose to build on the current architecture host"""
    images_dict = {}
    images_file_path = images_file_path if images_file_path else IMAGES_FILE_PATH
    assert not images_file_path.is_absolute()
    cur_dir = os.path.dirname(__file__)
    path_to_images_file = (
        repo_path if repo_path else Path(f"{cur_dir}/../..") / images_file_path
    )
    if path_to_images_file.exists():
        with open(path_to_images_file, "rb") as dict_file:
            images_dict = json.load(dict_file)
    else:
        logging.info(
            "Image file %s doesn't exist in repo %s", images_file_path, repo_path
        )

    return images_dict


def get_image_names(
    repo_path: Optional[Path] = None, images_file_path: Optional[Path] = None
) -> List[str]:
    images_dict = get_images_dict(repo_path, images_file_path)
    return [info["name"] for (_, info) in images_dict.items()]


def get_images_info() -> Dict[str, dict]:
    """
    get docker info from images.json in format "image name" : image_info
    """
    images_dict = get_images_dict()
    images_info: dict = {info["name"]: {"deps": []} for _, info in images_dict.items()}
    for path, image_info_reversed in images_dict.items():
        name = image_info_reversed["name"]
        dependents = image_info_reversed["dependent"]
        only_amd64 = "only_amd64" in image_info_reversed
        images_info[name]["path"] = path
        images_info[name]["only_amd64"] = only_amd64
        for dep_path in dependents:
            name_dep = images_dict[dep_path]["name"]
            images_info[name_dep]["deps"] += [name]
    assert len(images_dict) == len(images_info), "BUG!"
    return images_info


def get_images_oredered_list() -> List[DockerImageData]:
    """
    returns images in a sorted list so that dependents follow their dependees
    """
    images_info = get_images_info()

    ordered_images: List[DockerImageData] = []
    ordered_names: List[str] = []
    while len(ordered_names) < len(images_info):
        for name, info in images_info.items():
            if name in ordered_names:
                continue
            if all(dep in ordered_names for dep in info["deps"]):
                ordered_names += [name]
                parents = info["deps"]
                assert (
                    len(parents) < 2
                ), "FIXME: Multistage docker images are not supported in CI"
                ordered_images += [
                    DockerImageData(
                        path=info["path"],
                        repo=name,
                        only_amd64=info["only_amd64"],
                        parent=parents[0] if parents else None,
                    )
                ]
    return ordered_images
