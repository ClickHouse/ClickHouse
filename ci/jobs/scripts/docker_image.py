import json
import os
from typing import Optional

from ci.praktika.utils import Shell, Utils

DOCKER_TAG = os.getenv("DOCKER_TAG", "latest")


class DockerImage:
    def __init__(self, name: str, version: Optional[str] = None):
        self.name = name
        if version is None:
            self.version = "latest"
        else:
            self.version = version

    def __str__(self):
        return f"{self.name}:{self.version}"

    def __repr__(self):
        return f"DockerImage({self.name}:{self.version})"

    def pull_image(self):
        try:
            print(f"Pulling image {self} - start")
            Shell.check(f"docker pull {self}", strict=True)
            print(f"Pulling image {self} - done")
        except Exception as ex:
            print(f"Got exception pulling docker: {ex}")
            raise ex
        return self

    @staticmethod
    def get_docker_image(image_name: str) -> "DockerImage":
        assert DOCKER_TAG and isinstance(
            DOCKER_TAG, str
        ), "DOCKER_TAG env must be provided"
        if "{" in DOCKER_TAG:
            tags_map = json.loads(DOCKER_TAG)
            assert (
                image_name in tags_map
            ), f"Image name [{image_name}] does not exist in provided DOCKER_TAG json string"
            arch_suffix = "_arm" if Utils.is_arm() else "_amd"
            return DockerImage(image_name, tags_map[image_name] + arch_suffix)
        # DOCKER_TAG is a tag itself
        return DockerImage(image_name, DOCKER_TAG)
