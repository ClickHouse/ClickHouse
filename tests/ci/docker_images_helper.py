#!/usr/bin/env python3

import json
import logging
import os
from typing import Dict, List

IMAGES_FILE_PATH = "docker/images.json"

ImagesDict = Dict[str, dict]


def get_images_dict(repo_path: str, images_file_path: str) -> ImagesDict:
    """Return images suppose to build on the current architecture host"""
    images_dict = {}
    path_to_images_file = os.path.join(repo_path, images_file_path)
    if os.path.exists(path_to_images_file):
        with open(path_to_images_file, "rb") as dict_file:
            images_dict = json.load(dict_file)
    else:
        logging.info(
            "Image file %s doesn't exist in repo %s", images_file_path, repo_path
        )

    return images_dict


def get_image_names(repo_path: str, images_file_path: str) -> List[str]:
    images_dict = get_images_dict(repo_path, images_file_path)
    return [info["name"] for (_, info) in images_dict.items()]
