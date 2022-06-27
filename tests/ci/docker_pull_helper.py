#!/usr/bin/env python3

import os
import json
import time
import subprocess
import logging


class DockerImage:
    def __init__(self, name, version=None):
        self.name = name
        if version is None:
            self.version = "latest"
        else:
            self.version = version

    def __str__(self):
        return f"{self.name}:{self.version}"


def get_images_with_versions(reports_path, required_image, pull=True):
    images_path = None
    for root, _, files in os.walk(reports_path):
        for f in files:
            if f == "changed_images.json":
                images_path = os.path.join(root, "changed_images.json")
                break

    if not images_path:
        logging.info("Images file not found")
    else:
        logging.info("Images file path %s", images_path)

    if images_path is not None and os.path.exists(images_path):
        logging.info("Images file exists")
        with open(images_path, "r", encoding="utf-8") as images_fd:
            images = json.load(images_fd)
            logging.info("Got images %s", images)
    else:
        images = {}

    docker_images = []
    for image_name in required_image:
        docker_image = DockerImage(image_name)
        if image_name in images:
            docker_image.version = images[image_name]
        docker_images.append(docker_image)

    if pull:
        for docker_image in docker_images:
            for i in range(10):
                try:
                    logging.info("Pulling image %s", docker_image)
                    latest_error = subprocess.check_output(
                        f"docker pull {docker_image}",
                        stderr=subprocess.STDOUT,
                        shell=True,
                    )
                    break
                except Exception as ex:
                    time.sleep(i * 3)
                    logging.info("Got execption pulling docker %s", ex)
            else:
                raise Exception(
                    f"Cannot pull dockerhub for image docker pull {docker_image} because of {latest_error}"
                )

    return docker_images


def get_image_with_version(reports_path, image, pull=True):
    return get_images_with_versions(reports_path, [image], pull)[0]
