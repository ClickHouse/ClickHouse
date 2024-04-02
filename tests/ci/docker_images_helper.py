#!/usr/bin/env python3

import json
import logging
from pathlib import Path
from typing import Dict, List

IMAGES_FILE_PATH = Path("docker/images.json")

ImagesDict = Dict[str, dict]


def get_images_dict(repo_path: Path, images_file_path: Path) -> ImagesDict:
    """Return images suppose to build on the current architecture host"""
    images_dict = {}
    assert not images_file_path.is_absolute()
    path_to_images_file = repo_path / images_file_path
    if path_to_images_file.exists():
        with open(path_to_images_file, "rb") as dict_file:
            images_dict = json.load(dict_file)
    else:
        logging.info(
            "Image file %s doesn't exist in repo %s", images_file_path, repo_path
        )

    return images_dict


def get_image_names(repo_path: Path, images_file_path: Path) -> List[str]:
    images_dict = get_images_dict(repo_path, images_file_path)
    return [info["name"] for (_, info) in images_dict.items()]
